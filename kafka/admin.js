import { Kafka } from "kafkajs";
const kafka = new Kafka({ clientId: "kafka-service", brokers: ["localhost:9094"] });

const admin = kafka.admin();

const run = async () => {
  try {
    await admin.connect();
    console.log("Connected to Kafka");
    const existingTopics = await admin.listTopics();
    console.log("Existing topics:", existingTopics);
    const topicsToCreate = [
      { topic: "payment-successful", numPartitions: 3, replicationFactor: 3 },
      { topic: "order-successful", numPartitions: 3, replicationFactor: 3 },
      { topic: "email-successful", numPartitions: 3, replicationFactor: 3 },
    ];
    const newTopics = topicsToCreate.filter((t) => !existingTopics.includes(t.topic));
    if (newTopics.length > 0) {
      await admin.createTopics({ waitForLeaders: true, topics: newTopics });
      console.log("Created topics:", newTopics.map(t => t.topic));
    } else {
      console.log("All topics already exist");
    }
  } catch (error) {
    console.error("Error:", error.message, error.errors || "");
  } finally {
    await admin.disconnect();
  }
};
run();