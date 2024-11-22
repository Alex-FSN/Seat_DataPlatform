def execute_query_by_name(query_name, params, conn, sqlfile):
    dbt_project_path = "/usr/local/airflow/dags/"
    # Lee el archivo SQL
    file_name = dbt_project_path + sqlfile

    with open(file_name, "r") as file:
        contenido = file.read()
        consultas = contenido.split("--")
        consulta_sql = ""
        for consulta in consultas:
            if consulta.strip().startswith(f"@{query_name}"):
                consulta_sql = consulta.strip().split("\n", 1)[1].strip()
                break
        else:
            raise ValueError("No se encontró la consulta especificada en el archivo")
 
    # executa la query que se manda
    consulta_formateada = consulta_sql.format(**params)
    print(consulta_formateada)
    result = conn.execute(consulta_formateada)
    rows = result.fetchall()
    print((rows[0][0]))
    return rows[0][0]
