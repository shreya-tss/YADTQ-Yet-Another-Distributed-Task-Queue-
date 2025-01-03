# YADTQ-Yet-Another-Distributed-Task-Queue-
Project done as a part of an elective course Big Data (UE22CS342AA2) at Pes University
# Goal 
Distributed Task Queue-  is a system that efficiently manages tasks across multiple computers. It ensures tasks are executed asynchronously, making it ideal for applications requiring reliable, scalable, and parallel processing.asks are placed in a queue, and worker nodes pick them up when available. This approach allows for smooth task execution, even in dynamic environments where workers may join or leave. Popular tools like Celery serve as examples of such systems, managing distributed task execution seamlessly.The goal was to design a distributed task queue that co-ordinates across multiple worker nodes in a highly distributed setup using Kafka as the core communication service. Kafka was chosen over other messaging systems like RabbitMQ and ActiveMQ due to its ability to handle high-throughput data streams with low latency, built-in fault tolerance, and robust scalability. Additionally, Kafka's strong support for distributed architectures and persistent storage makes it well-suited for managing the complex, asynchronous workflows required in this system.

# System Design 


![image](https://github.com/user-attachments/assets/850f8303-93a5-4f5e-9741-ea52bc33c806)





# Features

Worker Assignment:

- Evenly distributes tasks among active workers.
- Ensures fault tolerance and task reassignment in case of worker failure.

Heartbeat Monitoring:

- Tracks active worker nodes.
- Ensures system reliability by identifying inactive nodes.

Task Status Tracking:

- Real-time updates for clients on task progress and outcomes.

# Example of working 
Task Example (in Kafka Queue):
### Submit a task
{
  "task-id": "12345abc",
  "task": "multiply",
  "args": [4, 5]
}

Result Example (in Result Backend):

{
  "d5750c0e-ed82": {
    "status": "success",
    "result": "3"
  }
}

# Code 
Below is a pseudocode to demonstrate the system 

![image](https://github.com/user-attachments/assets/8a52f8bc-4bce-499c-9ca5-4d510925fda0)



