import express from "express";
import fetch from "node-fetch";
const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_SERVICE_ROLE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY;

console.log("[gmail] env", {
  supabaseOrigin: SUPABASE_URL ? new URL(SUPABASE_URL).origin : null,
  hasServiceRole: !!SUPABASE_SERVICE_ROLE_KEY
});
const app = express();
const PORT = process.env.PORT || 3000;

app.use(express.json({ limit: "2mb" }));
async function sbGet(path) {
  const resp = await fetch(`${SUPABASE_URL}/rest/v1/${path}`, {
    method: "GET",
    headers: {
      apikey: SUPABASE_SERVICE_ROLE_KEY,
      Authorization: `Bearer ${SUPABASE_SERVICE_ROLE_KEY}`,
      Accept: "application/json",
    },
  });

  const text = await resp.text().catch(() => "");

  if (!resp.ok) {
    throw new Error(`Supabase GET failed ${resp.status}: ${text}`);
  }

  return text ? JSON.parse(text) : [];
}

async function sbPost(path, body, prefer = "return=representation") {
  const resp = await fetch(`${SUPABASE_URL}/rest/v1/${path}`, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      apikey: SUPABASE_SERVICE_ROLE_KEY,
      Authorization: `Bearer ${SUPABASE_SERVICE_ROLE_KEY}`,
      Prefer: prefer,
      Accept: "application/json",
    },
    body: JSON.stringify(body),
  });

  const text = await resp.text().catch(() => "");

  if (!resp.ok) {
    throw new Error(`Supabase POST failed ${resp.status}: ${text}`);
  }

  return text ? JSON.parse(text) : [];
}

async function sbPatch(path, body, prefer = "return=minimal") {
  const resp = await fetch(`${SUPABASE_URL}/rest/v1/${path}`, {
    method: "PATCH",
    headers: {
      "Content-Type": "application/json",
      apikey: SUPABASE_SERVICE_ROLE_KEY,
      Authorization: `Bearer ${SUPABASE_SERVICE_ROLE_KEY}`,
      Prefer: prefer,
      Accept: "application/json",
    },
    body: JSON.stringify(body),
  });

  const text = await resp.text().catch(() => "");

  if (!resp.ok) {
    throw new Error(`Supabase PATCH failed ${resp.status}: ${text}`);
  }

  return text ? JSON.parse(text) : [];
}
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

const conns = await sbGet(
  `email_connections?select=id,user_id,provider,status,email,access_token,refresh_token,expires_at&` +
  `provider=eq.google&email=eq.${encodeURIComponent(email)}&limit=1`
);

const conn = conns?.[0] || null;

if (!conn) {
  console.log("[gmail] no matching google connection found", { email });
  return;
}

if (conn.provider !== "google") {
  console.log("[gmail] matched connection is not google", {
    email,
    provider: conn.provider
  });
  return;
}

if (conn.status && ["disconnected", "revoked"].includes(conn.status)) {
  console.log("[gmail] connection disconnected/revoked", {
    email,
    status: conn.status
  });
  return;
}

if (!conn.refresh_token) {
  console.log("[gmail] missing refresh_token on connection", {
    connectionId: conn.id,
    email
  });
  return;
}

console.log("[gmail] resolved connection", {
  connectionId: conn.id,
  userId: conn.user_id,
  email: conn.email,
  status: conn.status
});

// Next step will process this

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
