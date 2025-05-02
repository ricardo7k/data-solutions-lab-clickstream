# Data Solutions Lab

This repository contains various data-related projects and solutions developed in the Data Solutions Lab.

## Projects

*   **Project 1:** [Link to Project 1](link_to_project_1) - Description of Project 1.
*   **Project 2:** [Link to Project 2](link_to_project_2) - Description of Project 2.
*   **Project 3:** [Link to Project 3](link_to_project_3) - Description of Project 3.

## Getting Started

To get started with any of the projects, follow the instructions provided in the respective project's README file.

* **Install envieronment python 3.13.3**
```bash
deactivate
rm -rf .venv/
python3.11 -m venv .venv
source .venv/bin/activate
pip install --upgrade pip setuptools
pip install 'apache-beam[gcp]'
pip install google-cloud-bigquery
pip install google-cloud-pubsub
pip install google-cloud-secret-manager
pip install google-cloud-composer
pip install flask
pip install "apache-airflow==2.10.5"
pip install apache-airflow-providers-google apache-airflow-providers-apache-beam
airflow db init

airflow users create \
    --username admin \
    --firstname Admin \
    --lastname User \
    --role Admin \
    --email ricardo7k@yahoo.com.br \
    --password admin
```
I we got error to run DirectRunner

```bash
pip cache purge
pip uninstall apache-beam
pip install 'apache-beam[gcp]'
```
* **Define environment variables**
```bash
export GOOGLE_CLOUD_PROJECT="dsl-clickstream"
export GCS_BUCKET_INPUT="dls-clickstream-data"
export PUBSUB_TOPIC_ID="ecommerce_clickstream"
export PUBSUB_SUB_PULL_ID="ecommerce_clickstreamd_pull_sub"
export PUBSUB_SUB_PUSH_ID="ecommerce_clickstreamd_push_sub"
export DATAFLOW_SERVICE_ACCOUNT="cloud-run@dsl-clickstream.iam.gserviceaccount.com"

gcloud config set project $GOOGLE_CLOUD_PROJECT
gcloud auth application-default login
```

* **Create bigquery DB**
```bash
python utils/create_ecommerce_bq.py ecommerce_clickstream
```

* **Create storage bucket**
```bash
gsutil mb -p $GOOGLE_CLOUD_PROJECT -l us-central1 gs://$GCS_BUCKET_INPUT

gsutil -m cp -r 'gs://challenge-lab-data-dar/**' gs://$GCS_BUCKET_INPUT/

gsutil mb -p $GOOGLE_CLOUD_PROJECT -l us-central1 gs://$GCS_BUCKET_INPUT-dataflow-temp

gsutil mb -p $GOOGLE_CLOUD_PROJECT -l us-central gs://$GCS_BUCKET_INPUT-dataflow-staging
```

* **Create pubsub topic and pull subscription**
```bash
gcloud pubsub topics create $PUBSUB_TOPIC_ID --project=$GOOGLE_CLOUD_PROJECT

gcloud pubsub subscriptions create $PUBSUB_SUB_PULL_ID \
 --topic=$PUBSUB_TOPIC_ID \
 --project=$GOOGLE_CLOUD_PROJECT \
 --ack-deadline=10
```

* **Create Secrets for cloud run**
```bash
gcloud secrets delete bigquery_dataset_id --project=$GOOGLE_CLOUD_PROJECT --quiet
gcloud secrets delete bigquery_visits_table_id --project=$GOOGLE_CLOUD_PROJECT --quiet
gcloud secrets delete bigquery_events_table_id --project=$GOOGLE_CLOUD_PROJECT --quiet
gcloud secrets delete bigquery_purchase_items_table_id --project=$GOOGLE_CLOUD_PROJECT --quiet

echo -n "ecommerce_clickstream" | gcloud secrets create bigquery_dataset_id --data-file=- --project=$GOOGLE_CLOUD_PROJECT
echo -n "visits" | gcloud secrets create bigquery_visits_table_id --data-file=- --project=$GOOGLE_CLOUD_PROJECT
echo -n "events" | gcloud secrets create bigquery_events_table_id --data-file=- --project=$GOOGLE_CLOUD_PROJECT
echo -n "purchase_items" | gcloud secrets create bigquery_purchase_items_table_id --data-file=- --project=$GOOGLE_CLOUD_PROJECT
```
* **Create local variables for cloud run local test**
```bash
chmod +x task3/subscribers/cloudrun_push/msg_emulator_local.sh
```

## Contributing

Contributions are welcome! Please read the contributing guidelines before submitting a pull request.

## License

This project is licensed under the [License Name] License - see the LICENSE.md file for details.
