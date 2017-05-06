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


class AM_Object(object):
    def __init__(self, o, f):
        self.o = o
        self.type = o["type"]
        self.connection = pyodbc.connect(f["connections"]["anchor_connection"]["conn_str"])

        self.sort_order

    def create(self):
        pass

    def init_load(self):
        pass

    def add(self, r):
        pass

    def commit_load(self):
        pass


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
            self.target_objects.append(AM_Object(o, f))
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
    load_anchors()
        '''
            CREATE TABLE #tmp_anchor (anchor_id)
            INSERT INTO #tmp_anchor
                VALUES (),(),(),()
            ...
            MERGE anchor t USING (SELECT DISTINCT anchor_id FROM #temp_anchors) s ON s.anchor_id = t.anchor_id
            WHEN NOT MATCHED THEN INSERT(anchor_Id) VALUES(s.anchor_id)
        '''
    load_knots()
        '''
            CREATE TABLE #tmp_knots (knot_val)
            INSERT INTO #tmp_knots
                VALUES (), (), (), ()
            ...
            MERGE knot t USING (SELECT DISTINCT knot_val FROM #temp_knot) s OM t.knot_val = s.knot_val
            WHEN NOT MATCHED THEN INSERT (knot_val) VALUES(s.knot_val)
        '''
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
