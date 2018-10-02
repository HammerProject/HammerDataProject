/home/hadoop/software/spark-2.3.0-bin-hadoop2.7/bin/spark-submit \
        --class org.hammer.shark.App \
        --master local[2] \
        --deploy-mode client \
        --driver-memory 3g \
        --name SHARK \
        --conf "spark.app.id=SHARK" \
        --files shark.conf \
        hammer-shark-0.0.1.jar \
        1_nosql.json search keywords 0.2 0.2 0.80 3 dataset_nosql index_nosql 10
