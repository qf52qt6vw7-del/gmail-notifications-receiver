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

    console.log("[gmail] raw notification", body);

    // ACK immediately (VERY IMPORTANT for Pub/Sub)
    res.status(200).send("ok");

    if (!body?.message?.data) {
      console.log("[gmail] missing message.data");
      return;
    }

    // Decode base64 PubSub payload
    const decoded = JSON.parse(
      Buffer.from(body.message.data, "base64").toString("utf8")
    );

    console.log("[gmail] decoded notification", decoded);

    /*
    Example decoded payload:

    {
      emailAddress: "user@gmail.com",
      historyId: "123456"
    }
    */

    const email = decoded?.emailAddress;
    const historyId = decoded?.historyId;

    if (!email || !historyId) {
      console.log("[gmail] missing emailAddress or historyId");
      return;
    }

    console.log("[gmail] notification parsed", {
      email,
      historyId
    });

    // Next step will process this

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
