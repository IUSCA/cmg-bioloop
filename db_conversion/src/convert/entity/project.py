def convert_projects(pg_cursor, mongo_db):
  print("convert_projects")
  with pg_cursor:
    projects = mongo_db.projects.find()
    for project in projects:
      print(f"Converting project: {project.get('name')}")
      # Insert project
      pg_cursor.execute(
        """
        INSERT INTO project (name)
        VALUES (%s)
        RETURNING id
        """,
        (
          project.get("name"),
          # todo - get browser_enabled value from Bioloop
          project.get("browser_enabled", False),
        )
      )
      project_id = pg_cursor.fetchone()[0]
      print(f"converted project: {project.get('name')}")

      # todo - avoid nested loop
      # Process dataproducts
      for dataproduct_id in project.get("dataproducts", []):
        # Fetch the dataproduct from MongoDB
        mongo_dataproduct = mongo_db.dataproduct.find_one({"_id": dataproduct_id})
        if mongo_dataproduct:
          print(f"Converting dataproduct: {dataproduct_name}")
          dataproduct_name = mongo_dataproduct.get("name")
          is_deleted = mongo_dataproduct.get("visible")

          # Find the corresponding dataset in Postgres
          pg_cursor.execute(
            """
            SELECT id FROM dataset
            WHERE name = %s AND is_deleted = %s AND type = 'DATA_PRODUCT'
            """,
            (dataproduct_name, is_deleted)
          )
          dataset_result = pg_cursor.fetchone()

          if dataset_result:
            dataset_id = dataset_result[0]
            # Create project_dataset association
            pg_cursor.execute(
              """
              INSERT INTO project_dataset (project_id, dataset_id)
              VALUES (%s, %s)
              """,
              (project_id, dataset_id)
            )
          else:
            print(f"Warning: Dataproduct not found for name: {dataproduct_name}")
        else:
          print(f"Warning: Dataproduct not found in MongoDB for id: {dataproduct_id}")
