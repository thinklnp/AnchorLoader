import pyodbc
import json


def get_meta(conn):
    crsr = conn["connection"].cursor()
    crsr.execute("SET NOCOUNT ON; INSERT INTO {0}.am_meta(dt) VALUES(GETDATE()) SELECT SCOPE_IDENTITY() met_id".format(conn["schema"]))
    met_id = crsr.fetchone().met_id
    crsr.commit()
    return met_id


def to_sql_str(a):
    try:
        float(a)
        return str(a)
    except ValueError:
        return "'" + a.replace("'", "''") + "'"


class AM_Object(object):
    def __init__(self, o, f):
        self.o = o
        self.f = f
        self.name = o["name"]
        self.data = []
        self.schema = f["connections"]["anchor_connection"]["schema"]
        self.catalog = f["connections"]["anchor_connection"]["database"]
        self.connection = pyodbc.connect(f["connections"]["anchor_connection"]["conn_str"])
        self.columns = []
        self.sort_order = None
        self.historical = False
        self.dict2row = {}

    def create(self):
        create_script = ("SET NOCOUNT ON; CREATE TABLE {scm}.{tbl} ({clmns}, met_id INT NOT NULL {cdt} ,PRIMARY KEY ({pks}))"
                         .format(scm=self.schema, tbl=self.name,
                         clmns=", ".join(["{0} {1} NOT NULL {2}".format(
                                            c["cl_name"]
                                            ,"BIGINT" if "a_obj" in c else c["cl_type"]
                                            ," REFERENCES {0}({1})".format(self.schema + "." + c["a_obj"].name, c["a_obj"].name + "_id")
                                                if "a_obj" in c else " IDENTITY(1,1) " if "src" not in c else ""
                                    ) for c in self.columns])
                        ,cdt= ", changedDt DATETIME2 NOT NULL DEFAULT GETDATE()" if self.historical else ""
                        ,pks=", ".join([c["cl_name"] for c in self.columns if "cl_pk" in c]) + ", changedDt DESC" if self.historical else ""))
        crsr = self.connection.cursor()
        if not crsr.tables(table=self.name, schema=self.schema, catalog=self.catalog).fetchone():
            crsr.execute(create_script)
            crsr.commit()

    def init_load(self, curs):
        self.data = []
        self.dict2row = {e[0]:ne for ne, e in enumerate(curs.description)}

    def add(self, r):
        pass

    def commit_load(self, metadata):
        tmp_cs = [{"cl_name": c["a_obj"].name if "a_obj" in c else c["src"],
                   "cl_type": c["a_obj"].o["column_type"] if "a_obj" in c else c["cl_type"],
                   "cl_a": c["a_obj"] if "a_obj" in c else "",
                   "cl_target": "XXX"} for c in self.columns if "a_obj" in c or "src" in c]
        curs = self.connection.cursor()
        ##Refack here!!
        curs.execure(
            "CREATE TABLE  #tmp_{0} ({1})".format(self.name, ",".join(["{0} {1}".format(c["cl_name"],c["cl_type"])
                                                                       for c in tmp_cs])))
        for sv in range(0, len(self.data), 900):
            curs.execute("INSERT INTO #tmp_{0} VALUES ".format(self.name)
                         + ", ".join(["({0})".format(",".join(to_sql_str(v))) for v in self.data[sv:sv + 900]]))


##refack!!!
        merge_script = ("SET NOCOUNT ON; MERGE {name} t USING (SELECT DISTINCT {clmns} FROM #tmp_{name}) s {joins} ON {j_clmns} "
                + " WHEN NOT MATCHED THEN INSERT({clmns},met_id) VALUES({s_clmns},{meta})"
                + " WHEN MATCHED {nm_clmns} THEN UPDATE SET {m_clmns}, met_id={meta}}" if not self.historical else ""
                ).format(name=self.name,
                         clmns=", ".join([c["cl_name"] for c in tmp_cs]),
                         joins=" ".join([" JOIN {0}.{1} {1} ON {1}.{1}_id = s.{1}_id".format(self.schema,c["cl_a"].name) for c in tmp_cs if c["cl_a"]])),
                         j_clmns=" AND ".join(["t.{0} = {1}.{2}".format(c["cl_target"],c["cl_a"].name if c["cl_a"] else "s", ) for c in tmp_cs])

        curs.execute(merge_script)

        curs.execute(("SET NOCOUNT ON; MERGE {name} t USING (SELECT DISTINCT {clmns} FROM #tmp_{name}) s {joins} ON {j_clmns} "
                + " WHEN NOT MATCHED THEN INSERT({clmns},met_id) VALUES({s_clmns},{meta})"
                + " WHEN MATCHED {nm_clmns} THEN UPDATE SET {m_clmns}, met_id={meta}}" if not self.historical else ""
                ).format(name=self.name,
                    clmns=",".join([c["cl_name"] for c in tmp_cs]),
                    s_clmns=",".join(["{0}.{1}".format(c.get("cl_ref_name","s"),c["cl_name"]) for c in self.columns]),
                    m_clmns=",".join(["{0}={1}.{0}".format(c["cl_name"],c.get("cl_ref_name","s"))
                                      for c in self.columns]),
                    nm_clmns=" ".join(["AND t.{0}<>{1}.{0}".format(c["cl_name"],c.get("cl_ref_name","s"))
                                       for c in self.columns if "cl_pk" not in c]),
                    j_clmns=" AND ".join(["t.{0} = {1}.{0}".format(c["cl_name"], c.get("cl_ref", "s"))
                                          for c in tmp_cs]),
                    joins=" ".join([" JOIN {0}.{1} {1} ON {1}.{1}_id = s.{1}_id".format(self.schema, c["cl_ref"])
                                    for c in tmp_cs if c["cl_ref"]]),
                    meta=str(metadata))
                )
        curs.commit()


def get_am_object(obj, fl):
    class Anchor(AM_Object):
        def __init__(self, o, f):
            super().__init__(o, f)
            self.sort_order = 1
            self.columns = [{
                                "cl_name":o["name"] + "_id",
                                "cl_type":"BIGINT", "cl_pk":0
                            },
                            {
                                "cl_name":"external_id",
                                "cl_type": o["column_type"],
                                "src":o["source_column"]
                            }]
        def add(self, r):
            self.data.append([r[self.dict2row[self.o["source_column"]]]])

    class Attribute(AM_Object):
        def __init__(self, o, f, hist=False):
            super().__init__(o, f)
            self.sort_order = 2
            self.historical = hist
            self.a = [t_o for t_o in f["anchors"] if t_o["name"] == o["anchor"]][0]
            self.name = self.a["name"] + "_" + o["name"]
            self.columns = [{
                                "cl_name": self.a["name"] + "_id",
                                "a_obj": Anchor(self.a, f),
                                "cl_pk":0
                            },
                            {
                                "cl_name": o["name"],
                                "cl_type": o["column_type"],
                                "src":o["source_column"]
                            }]

        def init_load(self, curs):
            super().init_load(curs)
            for c in self.columns:
                if "a_obj" in c: c["a_obj"].init_load(curs)

        def add(self, r):
            self.data.append([r[self.dict2row[self.a["source_column"]]], r[self.dict2row[self.o["source_column"]]]])
            for c in self.columns:
                if "a_obj" in c: c["a_obj"].add(r)

        def commit_load(self, metadata):
            for c in self.columns:
                if "a_obj" in c: c["a_obj"].commit_load(metadata)
            super().commit_load(metadata)

    ##TO_DO причесать Tie не забыть про удаление... Хотя может сейчас и забыть.
    class Tie(AM_Object):
        def __init__(self, o, f, hist=False):
            super().__init__(o, f)
            self.sort_order = 2
            self.historical = hist
            self.name = o["name"]
            pk_i = 0
            for c in o["columns"]:
                a = [x for x in f["anchors"] if x["name"] == c["name"]][0]
                cols = {
                        "cl_name": a["name"] + "_id",
                        "a_obj": Anchor([x for x in f["anchors"] if x["name"] == c["name"]][0], f)
                    }
                if c["type"] in ["anchor_pk"]:
                    cols.update({"cl_pk": pk_i})
                    pk_i += 1
                self.columns.append(cols)

        def init_load(self, curs):
            super().init_load(curs)
            for c in self.columns:
                if "a_obj" in c: c["a_obj"].init_load(curs)

        def add(self, r):
            self.data.append([r[self.dict2row[c["source_column"]]] for c in self.o["columns"]])
            for c in self.columns:
                if "a_obj" in c: c["a_obj"].add(r)

        def commit_load(self, metadata):
            for c in self.columns:
                if "a_obj" in c: c["a_obj"].commit_load(metadata)
            super().commit_load(metadata)

    if obj["type"] == "anchor": return Anchor(obj, fl)
    if obj["type"] == "tie": return Tie(obj, fl)
    if obj["type"] == "attribute": return Attribute(obj, fl)
    if obj["type"] == "historical_attribute": return Attribute(obj, fl, True)
    if obj["type"] == "historical_tie": return Tie(obj, fl, True)


class Source(object):
    def __init__(self, s, f):
        self.s = s
        self.name = s["name"]
        self.connection = pyodbc.connect(f["connections"]["source_connections"][s["connection"]])
        self.anchor_connection = pyodbc.connect(f["connections"]["anchor_connection"]["conn_str"])
        self.schema = f["connections"]["anchor_connection"]["schema"]
        self.source_columns = set()
        self.target_objects = []
        for o in [fo for fo in f["anchors"] + f["attributes"] + f["ties"] if fo["source"] == s["name"]]:
            for c in o.get("columns",[o]):
                self.source_columns.add(c["source_column"])
            self.target_objects.append(get_am_object(o, f))
        self.target_objects.sort(key=lambda x: x.sort_order)

    def get_rv(self):
        crsr = self.anchor_connection.cursor()
        crsr.execute("SELECT MAX({0}) rv FROM {2}.{1}_{0}".format(self.s["delta_field"],self.s["name"],self.schema))
        rows = crsr.fetchone()
        return rows[0] if rows[0] else 0

    def load_source(self, met_id):
        select_script = ("SELECT TOP {cnt} {clmns} FROM {src} WHERE {d_field} > CAST({m_rv} AS BINARY(8)) ORDER BY {d_field} "
                .format(cnt=10000, clmns=", ".join([c for c in self.source_columns]), src=self.s["source_table"], m_rv=self.get_rv(), d_field=self.s["delta_field"]))
        curs = self.connection.cursor()
        curs.execute(select_script)
        for x in self.target_objects:
            x.init_load()
        for r in curs:
            for x in self.target_objects:
                x.add(r)
        ao, bo = [], []
        for t in self.target_objects:
            if t.sort_order == 1:
                ao.append(t)
            else:
                bo.append(t)
        for x in ao:
            x.commit_load(met_id)
        for x in bo:
            x.commit_load(met_id)


def create_table(conn, t):
    crsr = conn["connection"].cursor()
    if not crsr.tables(table=t["name"], schema=conn["schema"], catalog=conn["database"]).fetchone():
        crsr.execute("CREATE TABLE " + conn["schema"] + "." + t["name"] + " ( "
                        + ", ".join([c_n + " " + c_t for c_n, c_t in t["columns"].items()])
                        + (", PRIMARY KEY (" + ", ".join(t["pk"]) + ")" if "pk" in t else "") + ")")
        crsr.commit()


def drop_table(conn, t):
    crsr = conn.cursor()
    if crsr.tables(table=t["name"]).fetchone():
        drop_script = "DROP TABLE " + t["schema"] + "." + t["name"]
        crsr.execute(drop_script)
        crsr.commit()


def create_db(conn, f):
    create_table(conn, {"name": "am_meta",
                        "columns": {"met_id": "INT NOT NULL PRIMARY KEY IDENTITY(1,1)",
                                    "dt": "DATETIME2 NOT NULL DEFAULT GETDATE()"}})
    for s_o in f["source_objects"]:
        create_table(conn, {"name": "{name}_{df}".format(name=s_o["name"], df=s_o["delta_field"]),
                            "columns": {s_o["delta_field"]: "BIGINT NOT NULL",
                                        "met_id": "INT NOT NULL PRIMARY KEY",
                                        "dt": "DATETIME2 NOT NULL DEFAULT GETDATE()"}})
    for o in f["objects"]:
        get_am_object(o, f).create()


def main():
    with open("anchor_modelling.json") as f_file:
        f = json.load(f_file)
    with pyodbc.connect(f["connections"]["anchor_connection"]["conn_str"]) as cn:
        conn_a = {"connection": cn,
                   "schema": f["connections"]["anchor_connection"]["schema"],
                   "database": f["connections"]["anchor_connection"]["database"]}
        create_db(conn_a, f)
        met_id = get_meta(conn_a)
    for s in f["source_objects"]:
        Source(s,f).load_source(met_id)

if __name__ == "__main__":
    main()
