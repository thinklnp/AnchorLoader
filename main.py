import pyodbc
import json
import datetime


def get_meta(conn):
    crsr = conn["connection"].cursor()
    crsr.execute("SET NOCOUNT ON; INSERT INTO {0}.am_meta(dt) VALUES(GETDATE()) SELECT SCOPE_IDENTITY() met_id".format(conn["schema"]))
    met_id = crsr.fetchone().met_id
    crsr.commit()
    return met_id


def to_sql_str(a):
    if isinstance(a, str):
        return "'" + a.replace("'", "''") + "'"
    elif isinstance(a, datetime.datetime):
        return "'" + a.strftime("%Y-%m-%d %H:%M:%S") + "'"
    else:
        return str(a)


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
        self.k = None
        self.dict2row = {}

    def create(self):
        crsr = self.connection.cursor()
        if not crsr.tables(table=self.name, schema=self.schema, catalog=self.catalog).fetchone():
            crsr.execute("SET NOCOUNT ON; CREATE TABLE {scm}.{tbl} ({clmns}, met_id INT NOT NULL {cdt} ,PRIMARY KEY ({pks}))"
                         .format(scm=self.schema, tbl=self.name,
                         clmns=", ".join([c["cl_name"] + " " + c["cl_type"] + " NOT NULL " +
                            " REFERENCES {0}({1})".format(self.schema + "." + c["cl_ref_name"],c["cl_ref_name"] + "_id")
                                          if "cl_ref_name" in c else "" for c in self.columns])
                        ,cdt= ", changedDt DATETIME2 NOT NULL DEFAULT GETDATE()" if self.historical else ""
                        ,pks=", ".join([c["cl_name"] for c in self.columns if "cl_pk" in c]) + ", changedDt DESC" if self.historical else "")
                         )
            crsr.commit()

    def init_load(self, curs):
        self.data = []
        self.dict2row = {e[0]:ne for ne, e in enumerate(curs.description)}

    def add(self, r):
        pass

    def commit_load(self, metadata):
        tmp_cs = [{"cl_name": c["cl_ref_name"] + "_value" if "cl_ref_name" in c else c["cl_name"],
                   "cl_type": c["cl_ref_type"] if "cl_ref_type" in c else c["cl_type"]} for c in self.columns]
        curs = self.connection.cursor()
        curs.execute(
            "CREATE TABLE  #tmp_{0} ({1})".format(self.name, ",".join(["{0} {1}".format(c["cl_name"],c["cl_type"])
                                                                       for c in tmp_cs])))
        for sv in range(0, len(self.data), 900):
            curs.execute("INSERT INTO #tmp_{0} VALUES ".format(self.name)
                         + ", ".join(["({0})".format(",".join(map(to_sql_str, v))) for v in self.data[sv:sv + 900]]))
            ##TODO What's th fuck.. where simple load?
        curs.execute(("SET NOCOUNT ON; MERGE {schema}.{name} t USING (SELECT DISTINCT {clmns} FROM #tmp_{name}) s {joins} ON {j_clmns} "
            + " WHEN NOT MATCHED THEN INSERT({clmns},met_id) VALUES({s_clmns},{meta})"
            + " WHEN MATCHED {nm_clmns} THEN UPDATE SET {m_clmns}, met_id={meta};" if not self.historical else ";"
            ).format(name=self.name, schema=self.schema,
                clmns=",".join([c["cl_name"] for c in self.columns]),
                s_clmns=",".join(["{0}.{1}".format(c.get("cl_ref_name","s"),c["cl_name"]) for c in self.columns]),
                m_clmns=",".join(["{0}={1}.{0}".format(c["cl_name"],c.get("cl_ref_name","s"))
                                  for c in self.columns]),
                nm_clmns=" ".join(["AND t.{0}<>{1}.{0}".format(c["cl_name"],c.get("cl_ref_name","s"))
                                   for c in self.columns if "cl_pk" not in c]),
                j_clmns=" AND ".join(["t.{0} = {1}.{0}".format(c["cl_name"], c.get("cl_ref_name", "s"))
                                      for c in self.columns if "cl_pk" in c]),
                joins=" ".join([" JOIN {0}.{1} {1} ON {1}.{1}_value = s.{1}_value".format(self.schema, c.get("cl_ref_name","s"))
                                for c in tmp_cs]) if self.k else "",
                meta=str(metadata))
            )
        curs.commit()


def get_am_object(obj, fl):
    class Anchor(AM_Object):
        def __init__(self, o, f):
            super().__init__(o, f)
            self.sort_order = 1
            self.columns = [{"cl_name":o["name"] + "_id", "cl_type": o["column_type"], "cl_pk":0}]

        def add(self, r):
            self.data.append([r]) # Не row а значение из attribute или tie

    class Knot(AM_Object):
        def __init__(self, o, f):
            super().__init__(o, f)
            self.sort_order = 1
            self.columns = [{"cl_name": o["name"] + "_id",
                             "cl_type": o["column_id_type"],
                             "cl_pk":0},
                            {"cl_name":  o["name"] + "_value",
                             "cl_type": o["column_type"]}]

        def add(self, r):
            self.data.append([r])  # Не row а значение из attribute или tie

    class Attribute(AM_Object):
        def __init__(self, o, f, hist=False):
            super().__init__(o, f)
            self.sort_order = 2
            self.historical = hist
            self.a = [t_o for t_o in f["objects"] if t_o["type"] == "anchor" and t_o["name"] == o["anchor"]][0]
            self.a_obj = Anchor(self.a, f)
            if "knot" in o:
                self.k = [k_o for k_o in f["objects"] if k_o["type"] == "knot" and k_o["name"] == o["knot"]][0]
                self.k_obj = Knot(self.k, f)
            self.name = self.a["name"] + "_" + o["name"]
            self.columns = [{
                                "cl_name": self.a["name"] + "_id",
                                "cl_type": self.a["column_type"],
                                "cl_ref_name": self.a["name"],
                                "cl_pk":0
                            }]
            if self.k:
                self.columns.append({
                    "cl_name": self.k["name"] + "_id",
                    "cl_type": self.k["column_id_type"],
                    "cl_ref_name": self.k["name"],
                    "cl_ref_type": self.k["column_type"]
                })
            else:
                self.columns.append({
                    "cl_name": o["column_name"],
                    "cl_type": o["column_type"]
                })

        def init_load(self, curs):
            super().init_load(curs)
            if self.k: self.k_obj.init_load(curs)
            self.a_obj.init_load(curs)

        def add(self, r):
            if self.k: self.k_obj.add(r[self.dict2row[self.o["source_column"]]])
            if self.a_obj: self.a_obj.add(r[self.dict2row[self.o["source_anchor"]]])
            self.data.append([r[self.dict2row[self.o["source_anchor"]]], r[self.dict2row[self.o["source_column"]]]])

        def commit_load(self, metadata):
            if self.k: self.k_obj.commit_load(metadata)
            self.a_obj.commit_load(metadata)
            super().commit_load(metadata)

    ##TO_DO причесать Tie не забыть про удаление... Хотя может сейчас и забыть.
    class Tie(AM_Object):
        def __init__(self, o, f, hist=False):
            super().__init__(o, f)
            self.sort_order = 2
            self.historical = hist
            self.name = o["name"]
            cr = {"anchor_pk": "anchor", "anchor": "anchor", "knot": "knot", "knot_pk": "knot"}
            pk_i = 0
            self.k_objs = []
            for c in o["columns"]:
                a = [x for x in f["objects"] if x["type"] == cr[c["type"]] and x["name"] == c["name"]][0]
                cols = {
                        "cl_name": c["name"] + "_id",
                        "cl_type": a["column_type"],
                        "cl_ref_name": a["name"],
                        "cl_ref_type": a["column_type"]
                    }
                if c["type"] in ["anchor_pk", "knot_pk"]:
                    cols.update({"cl_pk": pk_i})
                    pk_i += 1
                if c["type"] in ["knot","knot_pk"]:
                    cols.update({"k_obj": Knot([x for x in f["objects"] if x["type"] == "knot" and x["name"] == c["name"]][0],f),
                                 "k_source_column": c["source_column"]})
                if c["type"] in ["anchor","anchor_pk"]:
                    cols.update({"a_obj": Anchor([x for x in f["objects"] if x["type"] == "anchor" and x["name"] == c["name"]][0],f),
                                 "a_source_column": c["source_column"]})
                self.columns.append(cols)

        def init_load(self, curs):
            super().init_load(curs)
            for c in self.columns:
                if "k_obj" in c: c["k_obj"].init_load(curs)
                if "a_obj" in c: c["a_obj"].init_load(curs)

        def add(self, r):
            self.data.append([r[self.dict2row[c["source_column"]]] for c in self.o["columns"]])
            for c in self.columns:
                if "k_obj" in c: c["k_obj"].add([r[self.dict2row[c["k_source_column"]]]])
                if "a_obj" in c: c["a_obj"].add([r[self.dict2row[c["a_source_column"]]]])

        def commit_load(self, metadata):
            for c in self.columns:
                if "k_obj" in c: c["k_obj"].commit_load(metadata)
                if "a_obj" in c: c["a_obj"].commit_load(metadata)
            super().commit_load(metadata)

    if obj["type"] == "anchor": return Anchor(obj, fl)
    if obj["type"] == "knot": return Knot(obj, fl)
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
        for o in [fo for fo in f["objects"] if "source_name" in fo and fo["source_name"] == s["name"]]:
            for c in o.get("columns",[o]):
                self.source_columns.add(c["source_column"])
                if "source_anchor" in c: self.source_columns.add(c["source_anchor"])
            self.source_columns.add(o["source_column"])
            self.target_objects.append(get_am_object(o, f))
        self.target_objects.sort(key=lambda x: x.sort_order)

    def get_rv(self):
        crsr = self.anchor_connection.cursor()
        crsr.execute("SELECT MAX({0}) rv FROM {2}.{1}_{0}".format(self.s["delta_field"],self.s["name"],self.schema))
        rows = crsr.fetchone()
        return rows[0] if rows[0] else 0

    def load_source(self, met_id):
        curs = self.connection.cursor()
        curs.execute(
            "SELECT TOP {cnt} {clmns} FROM {src} WHERE {d_field} > CAST({m_rv} AS BINARY(8)) ORDER BY {d_field} "
                .format(cnt=10000, clmns=", ".join([c for c in self.source_columns]), src=self.s["source_table"], m_rv=self.get_rv(), d_field=self.s["delta_field"]))
        for x in self.target_objects:
            x.init_load(curs)
  #      map(lambda x: x.init_load(), self.target_objects)
        for r in curs:
            for x in self.target_objects:
                x.add(r)
  #          map(lambda x: x.add(r), self.target_objects)
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
   #     map(lambda x: x.commit_load(met_id), ao)
   #     map(lambda x: x.commit_load(met_id), bo)


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
