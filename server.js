import express from "express";

const app = express();
const PORT = process.env.PORT || 3000;

app.use(express.json({ limit: "2mb" }));

app.get("/", (req, res) => {
  res.status(200).send("gmail-notifications-receiver running");
});

/*
GMAIL PUBSUB PUSH ENDPOINT
*/
app.post("/email/gmail/notifications", async (req, res) => {
  try {
    const body = req.body;

    console.log("[gmail] notification received", {
      hasBody: !!body,
      time: new Date().toISOString(),
    });

    // ACK immediately
    res.status(200).send("ok");

  } catch (err) {
    console.log("[gmail] handler error", err?.message || err);

    if (!res.headersSent) {
      try { res.status(200).send("ok"); } catch {}
    }
  }
});

app.listen(PORT, () => {
  console.log(`gmail receiver listening on port ${PORT}`);
});
