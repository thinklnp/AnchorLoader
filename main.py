import pyodbc
import json


def create_table(conn, t):
    if not conn.tables(table=t["name"]).fetchone():
        create_script = "CREATE TABLE "
        create_script += conn.schema + "." + t["name"] + " ( "
        create_script += ", ".join([c_n + " " + c_t for c_n, c_t in t["columns"]])
        create_script += ")"
        conn.execute(create_script)
        conn.commit()


def create_db(conn, f):
    create_table(conn,{"name":"am_meta", "columns":{"met_id":"INT", "dt":"DATETIME2 DEFAULT GETDATE()"}})
    for s_o in f["source_objects"]:
        create_table(conn, {"name": s_o["name"] + "_" + s_o["delta_field"],
                                    "columns":{s_o["delta_field"]: "BIGINT",
                                               "met_id": "INT",
                                               "dt": "DATETIME 2 DEFAULT GETDATE()"}})
    for o in f["objects"]:
        if o["type"] == "anchor":
            create_table(conn, {"name": o["name"],
                                "columns": {
                                    o["name"] + "_id": o["column_type"],
                                    "met_id": "INT"
                                }})
        elif o["type"] == "attribute":
            a = [t_o for t_o in f["objects"] and t_o["type"] == "anchor" and t_o["name"] == o["anchor"]][0]
            create_table(conn, {"name": a["name"]+o["name"],
                                "columns":{
                                    a["name"]+"_id": a["column_type"],
                                    o["column_name"]: o["column_type"],
                                    "met_id": "INT"
                                }})


def main():
    with open("anchor_modelling.json") as f_file:
        f = json.load(f_file)
    conn_a = pyodbc.connect(f["connections"]["anchor_connection"]["conn_str"])
    conn_a.schema = f["connections"]["anchor_connection"]["schema"]
    create_db(conn_a, f)





if __name__ == "__main__":
    main()

    