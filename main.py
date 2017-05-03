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
        create_table(conn, {"name": s_o["name"] + "_" + s_o["delta_field"],
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
            cs.update({a["name"]+"_id": a["column_type"] + " NOT NULL PRIMARY KEY REFERENCES "
                       + conn["schema"] + "." + a["name"] + "(" + a["name"] + "_id) "})
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
            cr = {"anchor_pk": "anchor", "anchor": "anchor", "knot": "knot"}
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
            cr = {"anchor_pk": "anchor", "anchor": "anchor", "knot": "knot"}
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


def main():
    with open("anchor_modelling.json") as f_file:
        f = json.load(f_file)
    conn_a = pyodbc.connect(f["connections"]["anchor_connection"]["conn_str"])
    create_db({"connection": conn_a,
               "schema": f["connections"]["anchor_connection"]["schema"],
               "database": f["connections"]["anchor_connection"]["database"]}, f)


if __name__ == "__main__":
    main()
