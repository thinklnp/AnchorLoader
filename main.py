import pyodbc
import json


def create_table(conn, t):
    crsr = conn["connection"].cursor()
    if not crsr.tables(table=t["name"], schema=conn["schema"], catalog=conn["database"]).fetchone():
        create_script = "CREATE TABLE "
        create_script += conn["schema"] + "." + t["name"] + " ( "
        create_script += ", ".join([c_n + " " + c_t for c_n, c_t in t["columns"].items()])
        if t["pk"]:
            create_script += ", PRIMARY KEY (" + ", ".join(t["pk"]) + ")"
        create_script += ")"
        crsr.execute(create_script)
        crsr.commit()


def drop_table(conn, t):
    crsr = conn.cursor()
    if crsr.tables(table=t["name"]).fetchone():
        drop_script = "DROP TABLE " + t["schema"] + "." + t["name"]
        crsr.execute(drop_script)
        crsr.commit()


def create_db(conn, f):
    create_table(conn,{"name": "am_meta",
                       "columns": {"met_id": "INT NOT NULL PRIMARY KEY IDENTITY(1,1)",
                                   "dt":"DATETIME2 NOT NULL DEFAULT GETDATE()"}})
    for s_o in f["source_objects"]:
        create_table(conn, {"name": "{name}_{df}".format(name=s_o["name"], df=s_o["delta_field"]),
                            "columns": {s_o["delta_field"]: "BIGINT NOT NULL",
                                        "met_id": "INT NOT NULL PRIMARY KEY",
                                        "dt": "DATETIME2 NOT NULL DEFAULT GETDATE()"}})
    for o in f["objects"]:
        if o["type"] == "anchor":
            create_table(conn, {"name": o["name"],
                                "columns": {
                                    o["name"] + "_id": o["column_type"] + " NOT NULL PRIMARY KEY",
                                    "met_id": "INT NOT NULL"
                                }})
        elif o["type"] == "knot":
            create_table(conn, {"name": o["name"],
                                "columns": {
                                    o["name"] + "_id": o["column_id_type"] + " NOT NULL PRIMARY KEY",
                                    o["name"] + "_value": o["column_type"] + " NOT NULL",
                                    "met_id": " INT NOT NULL"
                                }})
        elif o["type"] == "attribute":
            a = [t_o for t_o in f["objects"] if t_o["type"] == "anchor" and t_o["name"] == o["anchor"]][0]
            cs = {}
            cs.update({a["name"]+"_id": "{col_t} NOT NULL PRIMARY KEY REFERENCES {schema}.{an}({an}_id)"
                                        .format(col_t = a["column_type"], schema=conn["schema"], an=a["name"])})
            if a["knot"]:
                k = [k_o for k_o in f["objects"] if k_o["type"] == "knot" and k_o["name"] == o["knot"]][0]
                cs.update({k["name"] + "_id": k["column_type"] + " NOT NULL REFERENCES "
                          + conn["schema"] + "." + k["name"] + "(" + k["name"] + "_id) "})
            else:
                cs.update({o["column_name"]: o["column_type"] + " NOT NULL"})
            cs.update({"met_id": "INT NOT NULL"})
            create_table(conn, {"name": a["name"] + "_" + o["name"],
                                "columns": cs
                                })
        elif o["type"] == "historical_attribute":
            a = [t_o for t_o in f["objects"] and t_o["type"] == "anchor" and t_o["name"] == o["anchor"]][0]
            cs = {}
            cs.update({a["name"]+"_id": a["column_type"] + " NOT NULL PRIMARY KEY REFERENCES "
                       + conn["schema"] + "." + a["name"] + "(" + a["name"] + "_id) "})
            if a["knot"]:
                k = [k_o for k_o in f["objects"] if k_o["type"] == "knot" and k_o["name"] == o["knot"]][0]
                cs.update({k["name"] + "_id": k["column_type"] + " NOT NULL REFERENCES "
                          + conn["schema"] + "." + k["name"] + "(" + k["name"] + "_id) "})
            else:
                cs.update({o["column_name"]: o["column_type"] + " NOT NULL"})
            cs.update({"changedDt": "DATETIME2 NOT NULL DEFAULT GETDATE()"})
            cs.update({"met_id": "INT NOT NULL"})
            create_table(conn, {"name": a["name"] + "_" + o["name"],
                                "columns": cs,
                                "pk": [a["name"] + "_id", "changedDt"]
                                })
        elif o["type"] == "tie":
            cr = {"anchor_pk": "anchor", "anchor": "anchor", "knot": "knot", "knot_pk": "knot"}
            cs = {}
            for c in o["columns"]:
                a = [x["column_type"] for x in f["objects"] if x["type"] == cr[c["type"]] and x["name"] == c["name"]][0]
                cs.update({c["name"] + "_id":
                           a["column_type"] + " NOT NULL REFERENCES "
                           + conn["schema"] + "." + a["name"] + "(" + a["name"] + "_id) "})
                cs.update({"met_id": "INT NOT NULL"})
            create_table(conn, {"name": o["name"],
                                "columns": cs,
                                "pk": [c["name"] + "_id" for c in o["columns"] if c["type"] == "anchor_pk"]})
        elif o["type"] == "historical_tie":
            cr = {"anchor_pk": "anchor", "anchor": "anchor", "knot": "knot", "knot_pk": "knot"}
            cs = {}
            for c in o["columns"]:
                a = [x["column_type"] for x in f["objects"] if x["type"] == cr[c["type"]] and x["name"] == c["name"]][0]
                cs.update({a["name"] + "_id":
                           a["column_type"] + " NOT NULL REFERENCES "
                           + conn["schema"] + "." + a["name"] + "(" + a["name"] + "_id) "})
                cs.update({"met_id": "INT NOT NULL"}),
                cs.update({"changedDt": "DATETIME2 NOT NULL DEFAULT GETDATE()"})
            create_table(conn, {"name": o["name"],
                                "columns": cs,
                                "pk": [c["name"] + "_id" for c in o["columns"]
                                       if c["type"] == "anchor_pk"].append("changedDt")
                                })


def get_meta(conn):
    crsr = conn["connection"].cursor()
    crsr.execute("INSERT INTO {0}.am_meta(dt) VALUES(GETDATE()) SELECT @@SCOPE_IDENTITY met_id".format(conn["schema"]))
    met_id = crsr.fetchone().met_id
    crsr.commit()
    return met_id

def str2sql(a): return "'" + a.replace("'","''") + "'"

def int2sql(a): return str(a)

class AM_Object(object):
    def __init__(self, o, f):
        self.o = o
        self.f = f
        self.name = o["name"]
        self.data = []
        self.schema = f["connections"]["anchor_connection"]["conn_str"]["schema"]
        self.catalog = f["connections"]["anchor_connection"]["conn_str"]["schema"]["database"]
        self.connection = pyodbc.connect(f["connections"]["anchor_connection"]["conn_str"])
        self.pk = []
        self.columns = []
        self.sort_order = None
        self.historical = False

    def create(self):
        crsr = self.connection.cursor()
        if not crsr.tables(table=self.name, schema=self.schema, catalog=self.catalog).fetchone():
            crsr.execute("CREATE TABLE {scm}.{tbl} ({clmns}, met_id INT NOT NULL {cdt} ,PRIMARY KEY ({pks}))"
                         .format(scm=self.schema, tbl=self.name,
                         clmns=", ".join([c["cl_name"] + " " + c["cl_type"] + " NOT NULL " +
                            " REFERENCES {0}({1})".format(self.schema + "." + c["cl_ref_name"],c["cl_ref_name"] + "_id")
                                          if "cl_ref_name" in c else "" for c in self.columns])
                        ,cdt= ", changedDt DATETIME2 NOT NULL DEFAULT GETDATE()" if self.historical else ""
                        ,pks=", ".join([c["cl_name"] for c in self.pk]) + ", changedDt DESC" if self.historical else "")
                         )
            crsr.commit()

    def init_load(self):
        self.data = []

    def add(self, r):
        pass

    def commit_load(self):
        tmp_cs = [{"cl_name": c["cl_ref_name"] + "_value" if "cl_ref_name" in c else c["cl_name"],
                   "cl_type": c["cl_ref_type"] if "cl_ref_type" in c else c["cl_type"]} for c in self.columns]
        curs = self.connection.cursor()
        curs.execure(
            "CREATE TABLE  #tmp_{0} ({1})" \
                .format(self.name, ",".join(["{0} {1}".format(c["cl_name"],c["cl_type"])  for c in self.tmp_cs])))
        for sv in range(0, len(self.data), 900):
            curs.execute("INSERT INTO #tmp_{0} VALUES ".format(self.name)
                         + ", ".join(["({0})".format(",".join(v)) for v in self.data[sv:sv + 900]]))
        if self.k:
            curs.execute(("MERGE {name} t USING (SELECT DISTINCT {clmns} FROM #tmp_{name}) s {joins} " +
                         # "  JOIN {3} k ON k.{3}_value = s.{2} +
                          " ON {j_clmns} " +
                          " WHEN NOT MATCHED THEN INSERT({clmns}) VALUES({s_clmns})"
                          + " WHEN MATCHED THEN UPDATE SET {m_clmns}" if not self.historical else ""
                          ).format(name=self.name,
                            clmns=",".join([c["cl_name"] for c in tmp_cs])),
                            s_clmns=",".join(["s." + c["cl_name"] for c in tmp_cs]),
                            ## Если JOIN то не так
                            m_clmns=",".join([ c["cl_name"] + "= t." + c["cl_name"] for c in self.columns]),
                            j_clmns=" AND ".join(["s." + c["cl_name"] + " = t." + c["cl_name"] for c in self.columns]),
                            joins=" ".join([" JOIN {0}.{1} {1} ON {1}.{1}_value = s.{1}_val".format(self.schema, c["cl_ref_name"])
                                            for c in tmp_cs])
                         )
        curs.commit()


def get_am_object(obj, fl):
    class Anchor(AM_Object):
        def __init__(self, o, f):
            super().__init__(o, f)
            self.sort_order = 1
            self.columns = [{"cl_name":o["name"] + "_id", "cl_type": o["column_type"]}]
            self.pk = [o["name"] + "_id"]

        def add(self, r):
            self.data.append(r[self.o["source_column"]])

        def commit_load(self):
            curs = self.connection.cursor()
            curs.execure("CREATE TABLE  #tmp_{0} ({0}_id {1})".format(self.name,self.o["column_type"]))
            for sv in range(0, len(self.data), 900):
                curs.execute("INSERT INTO #tmp_{0} VALUES ".format(self.name)
                             + ", ".join(["({0})".format(v) for v in self.data[sv:sv + 900]]))
            curs.execute("MERGE {0} t USING (SELECT DISTINCT {0}_id FROM #tmp_{0}) s ON s.{0}_id = t.{0}_id" \
                         "WHEN NOT MATCHED THEN INSERT({0}_Id) VALUES(s.{0}_id)".format(self.name))
            curs.commit()

    class Knot(AM_Object):
        def __init__(self, o, f):
            super().__init__(o, f)
            self.sort_order = 1
            self.columns = [{"cl_name": o["name"] + "_id",
                             "cl_type": o["column_id_type"]},
                            {"cl_name":  o["name"] + "_value",
                             "cl_type": o["column_type"]}]
            self.pk = [o["name"] + "_id"]

        def add(self, r):
            self.data.append(r)  # Не row а значение из attribute или tie

        def commit_load(self):
            curs = self.connection.cursor()
            curs.execure("CREATE TABLE  #tmp_{0} ({0}_value {1})".format(self.name, self.o["column_type"]))
            for sv in range(0, len(self.data), 900):
                curs.execute("INSERT INTO #tmp_{0} VALUES ".format(self.name)
                             + ", ".join(["({0})".format(v) for v in self.data[sv:sv + 900]]))
            curs.execute("MERGE {0} t USING (SELECT DISTINCT {0}_value FROM #tmp_{0}) s ON s.{0}_value = t.{0}_value" \
                         "WHEN NOT MATCHED THEN INSERT({0}_value) VALUES(s.{0}_value)".format(self.name))
            curs.commit()

    class Attribute(AM_Object):
        def __init__(self, o, f, hist=False):
            super().__init__(o, f)
            self.sort_order = 2
            self.historical = hist
            #TO_DO добавить закидывание строк в кноты
            self.a = [t_o for t_o in f["objects"] if t_o["type"] == "anchor" and t_o["name"] == o["anchor"]][0]
            if "knot" in o:
                self.k = [k_o for k_o in f["objects"] if k_o["type"] == "knot" and k_o["name"] == o["knot"]][0]
                self.k_obj = Knot(self.k, f)
                self.column_name = self.k["name"]+ "_value"
                self.column_type = self.k["column_type"]
            else:
                self.k = None
                self.column_name = self.o["column_name"]
                self.column_type = self.o["column_type"]
            self.name = self.a["name"] + "_" + o["name"]
            self.columns = [{
                                "cl_name": self.a["name"] + "_id",
                                "cl_tpye": self.a["column_type"],
                                "cl_ref_name": self.a["name"]
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
            self.pk = [self.a["name"] + "_id"]

        def init_load(self):
            self.data = []
            self.k_obj.init_load()

        def add(self, r):
            if self.k: self.k_obj.add(r[self.o["source_column"]])
            self.data.append((r[self.a["source_column"]], r[self.o["source_column"]]))

        def commit_load(self):
            if self.k: self.k_obj.commit_load()
            curs = self.connection.cursor()
            curs.execure(
                    "CREATE TABLE  #tmp_{0} ({1}_id {2}, {3} {4})" \
                    .format(self.name, self.a["name"], self.a["column_type"],
                            self.column_name, self.column_type ))
            for sv in range(0, len(self.data), 900):
                    curs.execute("INSERT INTO #tmp_{0}  VALUES ".format(self.name)
                             + ", ".join(["({0}, {1})".format(v[0], v[1]) for v in self.data[sv:sv + 900]]))
            if self.k:
                curs.execute(("MERGE {0} t USING (SELECT DISTINCT {1}_id, {2} FROM #tmp_{1}) s " +
                             "  JOIN {3} k ON k.{3}_value = s.{2} ON s.{1}_id = t.{1}_id " +
                             " WHEN NOT MATCHED THEN INSERT({1}_id, {3}_id) VALUES(s.{1}_id, k.{3}_id)" +
                             " WHEN MATCHED THEN UPDATE SET {3}_id = k.{3}_id" if not self.historical else ""
                              ).format(self.name, self.a["name"], self.column_name, self.k["name"]))
            else:
                curs.execute(("MERGE {0} t USING (SELECT DISTINCT {1}_id, {2} FROM #tmp_{1}) s ON s.{1}_id = t.{1}_id" +
                         " WHEN NOT MATCHED THEN INSERT({1}_id, {2}) VALUES(s.{1}_id, {2})" +
                         " WHEN MATCHED THEN UPDATE SET {2} = s.{2}" if not self.historical else ""
                              ).format(self.name, self.a["name"], self.o["column_name"]))
            curs.commit()

#TO_DO причесать Tie не забыть про удаление... Хотя может сейчас и забыть.
    class Tie(AM_Object):
        def __init__(self, o, f, hist=False):
            super().__init__(o, f)
            self.sort_order = 2
            self.historical = hist
            self.name = o["name"]
            cr = {"anchor_pk": "anchor", "anchor": "anchor", "knot": "knot", "knot_pk": "knot"}
            cs = []
            for c in o["columns"]:
                a = [x["column_type"] for x in f["objects"] if x["type"] == cr[c["type"]] and x["name"] == c["name"]][0]
                self.columns.append({
                    "cl_name": c["name"] + "_id",
                    "cl_type": a["column_type"],
                    "cl_ref_name": a["name"],
                    "cl_ref_type": a["column_type"]
                })
            #TO_DO knot_pk to keys
            self.pk = [c["name"] + "_id" for c in o["columns"] if c["type"] == "anchor_pk"]

        def add(self, r):
            self.data.append([r[c["source_column"]] for c in self.o["columns"]])

        def commit_load(self):
            #if self.k: self.k_obj.commit_load()
            curs = self.connection.cursor()
            curs.execure(
                    "CREATE TABLE  #tmp_{0} ({1})" \
                    .format(self.name, ",".join([c_n + " "+ c_t for c_n, c_t, c_k in self.tmp_columns]))  )
            for sv in range(0, len(self.data), 900):
                    #TO_DO кавычки для строковых данных!!!
                    curs.execute("INSERT INTO #tmp_{0}  VALUES ".format(self.name)
                             + ", ".join(["({0})".format(",".join(v)) for v in self.data[sv:sv + 900]]))

           # if self.k:
           #     curs.execute(("MERGE {0} t USING (SELECT DISTINCT {1}_id, {2} FROM #tmp_{1}) s " +
           #                  "  JOIN {3} k ON k.{3}_value = s.{2} ON s.{1}_id = t.{1}_id " +
           #                  " WHEN NOT MATCHED THEN INSERT({1}_id, {3}_id) VALUES(s.{1}_id, k.{3}_id)" +
           #                  " WHEN MATCHED THEN UPDATE SET {3}_id = k.{3}_id" if not self.historical else ""
           #                   ).format(self.name, self.a["name"], self.column_name, self.k["name"]))
           # else:

                    #TO_DO сделать с knotами
                    #TO_DO перенести в родителя
                curs.execute(("MERGE {name} t USING (SELECT DISTINCT {tmp_clmns} FROM #tmp_{name}) s ON {joins}" +
                         " WHEN NOT MATCHED THEN INSERT({tmp_clmns}) VALUES({s_tmp_clmns})" +
                         " WHEN MATCHED THEN UPDATE SET {u_tmp_clmns}" if not self.historical else ""
                              ).format(name=self.name, tmp_clmns=",".join([c[0] for c in self.tmp_columns]),
                                       s_tmp_clmns=",".join(["s." + c[0] for c in self.tmp_columns]),
                                       joins=" AND ".join(["t." + c[0] + "=s." + c[0] for c in self.tmp_columns]),
                                       u_tmp_clmns=",".join([c[0] + "=s." + c[0] for c in self.tmp_columns])))
            curs.commit()
            pass  # TO_DO dodelyat zagruzku tiev

    if obj["type"] == "anchor": return Anchor(obj, fl)
    if obj["type"] == "knot": return Knot(obj, fl)
    if obj["type"] == "tie": return Tie(obj, fl)
    if obj["type"] == "attribute": return Attribute(obj, fl)
    if obj["type"] == "historical_attribute": return Attribute(obj, fl, True)




class Source(object):
    def __init__(self, s, f):
        self.s = s
        self.name = s["name"]
        self.connection = pyodbc.connect(f["connections"]["source_connections"][s["connection"]])
        self.anchor_connection = pyodbc.connect(f["connections"]["anchor_connection"]["conn_str"])
        self.source_columns = set()
        self.target_objects = []
        for o in [fo for fo in f["objects"] if fo["source_name"] == s["name"]]:
            for c in o["columns"]:
                self.source_columns.add(c["source_column"])
            self.source_columns.add(o["source_column"])
            self.target_objects.append(get_am_object(o, f))
        self.target_objects.sort(key=lambda x: x.sort_order)

    def get_rv(self):
        crsr = self.anchor_connection.cursor()
        crsr.execute("SELECT MAX({0}) rv FROM {1}_{0}".format(self.s["delta_field"],self.s["name"]))
        return crsr.fetchone().rv

    def load_source(self):
        curs = self.connection.cursor()
        curs.execute(
            "SELECT TOP {cnt} {clmns} FROM {src} WHERE rv > CAST({m_rv} AS BINARY(8)) "
                .format(cnt=10000, clmns=", ".join([c for c in self.source_columns])
                        , src=self.s["source_table"], m_rv=self.get_rv))
        map(lambda x: x.init_load(), self.target_objects)
        for r in curs:
            map(lambda x: x.add(r), self.target_objects)
        ao, bo = [], []
        for t in self.target_objects:
            if t.sort_order == 1:
                ao.append(t)
            else:
                bo.append(t)
        map(lambda x: x.commit_load, ao)
        map(lambda x: x.commit_load, bo)


def load_source(s, conn_a, f, rv, met_id, cnt = 10000, par_id = None):
    columns = set()
    for o in [fo for fo in f["objects"] if fo["source_name"] == s["name"]]:
        for c in o["columns"]:
            columns.add(c["source_column"])
        columns.add(o["source_column"])
    curs = pyodbc.connect(f["connections"]["source_connections"][s["connection"]]).cursor()
    curs.execute(
        "SELECT TOP {cnt} {clmns} FROM {src} WHERE rv > CAST({m_rv} AS BINARY(8)) "
        .format(cnt=cnt, clmns=", ".join([c for c in columns]), src=s["source_table"], m_rv=rv))
    rows = curs.fetchall()

    load_attributes()
        '''
            CREATE TABLE #tmp_attributes (anchor_id, attribute)
            INSERT INTO #tmp_attributes
                VALUES (,), (,), (,), (,)
            ...
            MERGE attribute t USING (SELECT DISTINCT anchor_id, attribute) s ON s.anchor_id = t.anchor_id
            WHEN NOT MATCHED THEN INSERT (anchor_id,attribute) VALUES (s.anchor_id, s.attribute)
            WHEN MATCHED THEN UPDATE SET attribute = s.attribute
        '''
    load_historical_attributes()
        '''
            CREATE TABLE #tmp_attributes (anchor_id, attribute)
            INSERT INTO #tmp_attributes
                VALUES (,), (,), (,), (,)
            ...
            MERGE attribute t USING (SELECT DISTINCT anchor_id, attribute) s ON s.anchor_id = t.anchor_id AND s.attribute = t.attribute
            WHEN NOT MATCHED THEN INSERT (anchor_id,attribute) VALUES (s.anchor_id, s.attribute)
        '''
    load_knotted_attributes()
        '''
            CREATE TABLE #tmp_attributes (anchor_id, attribute)
            INSERT INTO #tmp_attributes  (anchor_id, attribute)
                VALUES (,), (,), (,), (,)
            ...

            MERGE attribute t USING
                            (SELECT DISTINCT anchor_id, attribute) x
                             JOIN knot k ON k.knot_val = x.attribute
                        s ON s.anchor_id = t.anchor_id
            WHEN NOT MATCHED THEN INSERT (anchor_id,knot_id) VALUES (x.anchor_id, k.knot_id)
            WHEN MATCHED THEN UPDATE SET knot_id = k.knot_id
        '''
    load_historical_knotted_attributes()
        '''
            CREATE TABLE #tmp_attributes (anchor_id, attribute)
            INSERT INTO #tmp_attributes  (anchor_id, attribute)
                VALUES (,), (,), (,), (,)
            ...

            MERGE attribute t USING
                            (SELECT DISTINCT anchor_id, attribute) x
                             JOIN knot k ON k.knot_val = x.attribute
                        s ON s.anchor_id = t.anchor_id AND s.knot_id = k.knot_id
            WHEN NOT MATCHED THEN INSERT (anchor_id,knot_id) VALUES (x.anchor_id, k.knot_id)
        '''
    load_ties()
        '''
            CREATE TABLE #tmp_tie (anchor1_id, anchor2_id..., knot1, knot2 ...)
            INSERT INTO #tmp_attributes  (anchor1_id, anchor2_id..., knot1, knot2 ...)
                VALUES (,..), (,..), (,..), (,..)
            ...

            MERGE tie t USING
                            (SELECT DISTINCT anchor1_id, anchor2_id,...
                                    ,knot1 ,knot2 ..) x
                             JOIN knot1 k1 ON k1.knot_val = x.knot1
                             JOIN knot2 k2 ON k2.knot_val = x.knot2
                             ...
                        s ON s.anchor1_id = t.anchor1_id AND s.anchor2_id = t.acnhor2_id ...(only pks)
            WHEN NOT MATCHED THEN INSERT (anchor1_id, anchor2_id..., knot1_id, knot2_id ...)
                                VALUES (x.anchor1_id, x.anchor2_id..., k1.knot_id, k2.knot_id ...)
            WHEN MATCHED THEN UPDATE
                SET knot1_id = k1.knot_id
                    knot2_id = k2.knot_id
                    ...
        '''
    load_historical_ties()
        '''
            CREATE TABLE #tmp_tie (anchor1_id, anchor2_id..., knot1, knot2 ...)
            INSERT INTO #tmp_attributes  (anchor1_id, anchor2_id..., knot1, knot2 ...)
                VALUES (,..), (,..), (,..), (,..)
            ...

            MERGE tie t USING
                            (SELECT DISTINCT anchor1_id, anchor2_id,...
                                    ,knot1 ,knot2 ..) x
                             JOIN knot1 k1 ON k1.knot_val = x.knot1
                             JOIN knot2 k2 ON k2.knot_val = x.knot2
                             ...
                        s ON s.anchor1_id = t.anchor1_id AND s.anchor2_id = t.acnhor2_id ...(all)
            WHEN NOT MATCHED THEN INSERT (anchor1_id, anchor2_id..., knot1_id, knot2_id ...)
                                VALUES (x.anchor1_id, x.anchor2_id..., k1.knot_id, k2.knot_id ...)
        '''
    pass


def main():
    with open("anchor_modelling.json") as f_file:
        f = json.load(f_file)
    conn_a = pyodbc.connect(f["connections"]["anchor_connection"]["conn_str"])
    create_db({"connection": conn_a,
               "schema": f["connections"]["anchor_connection"]["schema"],
               "database": f["connections"]["anchor_connection"]["database"]}, f)
    met_id = get_meta(conn_a)
    for s in f["source_objects"]:
        load_source(s, conn_a, f, get_rv(s,conn_a), met_id)

if __name__ == "__main__":
    main()
