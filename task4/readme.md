upload files from fucction trigger folder to cloud run function changing the values of composer info

add trigger on cloud composer functions to run after a file upload to cloud_storage bucket

# Permisson to run bash script
chmod +x run_composer.sh

# Create composer environment
gcloud composer environments create maestro-clickstream-pipe-3 \
 --location us-central1 \
 --image-version=composer-3-airflow-2.10.5-build.0 \
 --service-account=cloud-run@dsl-clickstream.iam.gserviceaccount.com

# Copy metadata
gsutil cp task4/metadata.json gs://dls-clickstream-data-dataflow-temp/template/metadata.json

# Create flex template
gcloud dataflow flex-template build "gs://dls-clickstream-data-dataflow-temp/template/metadata.json" \
--image "us-central1-docker.pkg.dev/dsl-clickstream/ecommerce-apps/composer_subscriber_pull" \
--sdk-language PYTHON \
--metadata-file "task4//metadata.json" \
--project $GOOGLE_CLOUD_PROJECT

# create composer container image with task1/ecommerce_pipeline.py
cp task1/ecommerce_pipeline.py task4/
cd task4/
gcloud builds submit --tag us-central1-docker.pkg.dev/dsl-clickstream/ecommerce-apps/composer_subscriber_pull

gsutil cp ./task4/ecommerce_pipeline_dag.py gs://us-central1-maestro-clickst-7c5386de-bucket/dags/ecommerce_pipeline_dag.py

us-central1-maestro-clickst-22547b29-bucket/dags