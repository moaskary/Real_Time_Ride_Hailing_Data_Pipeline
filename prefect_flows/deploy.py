from prefect.deployments import Deployment
from prefect.server.schemas.schedules import IntervalSchedule
from prefect.infrastructure.process import Process


# Import your flow function from the script where it is defined
from producer.run_producer import producer_flow
from spark_jobs.streaming_job import spark_streaming_flow

# Create a deployment for the producer flow
producer_deployment = Deployment.build_from_flow(
    flow=producer_flow,
    name="Streaming Data Producer",
    # This deployment will run the flow in a local process
    infrastructure=Process(), 
    # The work queue name is how an agent knows which deployments to pick up
    work_queue_name="streaming-pipeline",
    # You can add parameters here that will be used for this specific deployment
    parameters={
        "rate": 5 # Let's set a default rate of 5 events/sec for this deployment
    }
)

# --- Spark Deployment (New) ---
spark_deployment = Deployment.build_from_flow(
    flow=spark_streaming_flow,
    name="Spark Streaming Job",
    infrastructure=Process(),
    work_queue_name="streaming-pipeline"
)

if __name__ == "__main__":
    # The apply() method registers this deployment with the Prefect API server.
    producer_deployment.apply()
    spark_deployment.apply()
    
    print("Producer deployment has been applied! You can now run it from the Prefect UI.")

    # We will add the Spark job deployment here later.