python3 ./repeater.py --input gs://activate-beam-demo/input/svejk.txt --output gs://activate-beam-demo/output-mio  \
    --runner DataflowRunner \
    --project activate-data \
    --region europe-west1 \
    --temp_locatio gs://activate-beam-demo/temp/ \
    --stagging_location gs://activate-beam-demo/staging \
    --job_name=job-svejk-repeat-mio \
    --usePublicIps=false \
    --count=1000000