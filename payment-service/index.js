import express from "express";
import cors from "cors";
import { Kafka, Partitioners } from "kafkajs";

const app = express();

app.use(
  cors({
    origin: "http://localhost:3000",
  })
);
app.use(express.json());

const kafka = new Kafka({
  clientId: "payment-service",
  brokers: ["localhost:9094", "localhost:9095", "localhost:9096"],
});

const producer = kafka.producer({ createPartitioner: Partitioners.LegacyPartitioner }); // Fix KafkaJS warning

const connectToKafka = async () => {
  try {
    await producer.connect();
    console.log("Producer connected!");
  } catch (err) {
    console.error("Error connecting to Kafka:", err);
    process.exit(1); // Exit if Kafka connection fails
  }
};

app.post("/payment-service", async (req, res) => {
  try {
    const { cart } = req.body;
    if (!cart || !Array.isArray(cart)) {
      return res.status(400).json({ error: "Invalid or missing cart" });
    }
    const userId = "123"; // TODO: Implement real user ID logic

    // Send to Kafka
    await producer.send({
      topic: "payment-successful",
      messages: [{ value: JSON.stringify({ userId, cart }) }],
    });
    console.log("Message sent to payment-successful:", { userId, cart });

    // Simulate payment processing delay
    setTimeout(() => {
      res.status(200).json({
		token: "qwer1234",
        message: "Payment successful - format JSON"
      });
    }, 3000);
  } catch (error) {
    console.error("Error in /payment-service:", error);
    res.status(500).json({ error: "Internal server error", details: error.message });
  }
});

app.use((err, req, res, next) => {
  console.error("Global error:", err);
  res.status(err.status || 500).json({ error: err.message || "Internal server error" });
});

app.listen(8000, () => {
  connectToKafka();
  console.log("Payment service is running on port 8000");
});