from google.cloud import bigquery


class BigQueryWrapper():
    def __init__(self, project_name):
        self.client = bigquery.Client(project=project_name)

    def create_dataset(self, dataset_id):
        full_dataset_id = "{}.{}".format(self.client.project, dataset_id)
        dataset = bigquery.Dataset(full_dataset_id)
        dataset.location = "us-east1"
        dataset = self.client.create_dataset(dataset)  # Make an API request.
        print("Created dataset {}.{}".format(
            self.client.project, dataset.full_dataset_id))

    def create_table_from_storage(self, table_id, dataset, storage_uri):
        dataset_id = dataset
        table_id = table_id
        uri = storage_uri

        dataset_ref = self.client.dataset(dataset_id)

        job_config = bigquery.LoadJobConfig()
        job_config.autodetect = True
        job_config.source_format = bigquery.SourceFormat.CSV

        load_job = self.client.load_table_from_uri(
            uri, dataset_ref.table(table_id), job_config=job_config
        )  # API request
        print("Starting job {}".format(load_job.job_id))

        load_job.result()  # Waits for table load to complete.
        print("Job finished.")

        destination_table = self.client.get_table(dataset_ref.table(table_id))
        print("Loaded {} rows.".format(destination_table.num_rows))

    def query(self, query):
        # QUERY = (
        #     'SELECT * FROM {}.{}'.format(dataset_id, table_id))
        query_job = self.client.query(query)  # API request
        rows = query_job.result()  # Waits for query to finish

        for row in rows:
            print(row)


if __name__ == "__main__":
    project_name = "festive-magpie-279021"
    dataset_id = "teste_dataset"
    table_id = "comp_boss"
    storage_uri = "gs://teste_kaio/comp_boss.csv"
    wrapper = BigQueryWrapper(project_name)
    wrapper.create_dataset(dataset_id)
    wrapper.create_table_from_storage(table_id, dataset_id, storage_uri)
    wrapper.query("SELECT * FROM {}.{}".format(dataset_id, table_id))
