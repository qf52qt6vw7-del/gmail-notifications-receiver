import express from "express";
import fetch from "node-fetch";
const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_SERVICE_ROLE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY;

const GOOGLE_CLIENT_ID = process.env.GOOGLE_CLIENT_ID;
const GOOGLE_CLIENT_SECRET = process.env.GOOGLE_CLIENT_SECRET;

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
async function refreshGoogleAccessToken(refreshToken) {
  if (!GOOGLE_CLIENT_ID || !GOOGLE_CLIENT_SECRET) {
    throw new Error("Missing GOOGLE_CLIENT_ID / GOOGLE_CLIENT_SECRET");
  }

  if (!refreshToken) {
    throw new Error("Missing refresh_token");
  }

  const form = new URLSearchParams();

  form.set("client_id", GOOGLE_CLIENT_ID);
  form.set("client_secret", GOOGLE_CLIENT_SECRET);
  form.set("refresh_token", refreshToken);
  form.set("grant_type", "refresh_token");

  const resp = await fetch("https://oauth2.googleapis.com/token", {
    method: "POST",
    headers: {
      "Content-Type": "application/x-www-form-urlencoded"
    },
    body: form.toString()
  });

  const text = await resp.text().catch(() => "");

  if (!resp.ok) {
    throw new Error(`Google token refresh failed ${resp.status}: ${text}`);
  }

  const json = text ? JSON.parse(text) : null;

  const access_token = json?.access_token || null;
  const expires_in = Number(json?.expires_in || 0);

  if (!access_token) {
    throw new Error("Google refresh returned no access_token");
  }

  const expires_at = expires_in
    ? new Date(Date.now() + expires_in * 1000).toISOString()
    : null;

  return { access_token, expires_at };
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
  `email_connections?select=id,user_id,provider,status,email,access_token,refresh_token,expires_at,gmail_history_id&` +
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
let accessToken = conn.access_token;

const expiresAt = conn.expires_at ? new Date(conn.expires_at).getTime() : 0;

if (!accessToken || Date.now() >= expiresAt - 60000) {
  console.log("[gmail] refreshing google access token", {
    connectionId: conn.id
  });

  const refreshed = await refreshGoogleAccessToken(conn.refresh_token);

  accessToken = refreshed.access_token;

  await sbPatch(
    `email_connections?id=eq.${encodeURIComponent(conn.id)}`,
    {
      access_token: refreshed.access_token,
      expires_at: refreshed.expires_at
    },
    "return=minimal"
  );

  console.log("[gmail] token refreshed", {
    connectionId: conn.id
  });
  const historyResp = await fetch(
  `https://gmail.googleapis.com/gmail/v1/users/me/history?` +
  `startHistoryId=${encodeURIComponent(historyId)}&` +
  `historyTypes=messageAdded&labelId=INBOX`,
  {
    method: "GET",
    headers: {
      Authorization: `Bearer ${accessToken}`,
      Accept: "application/json"
    }
  }
);

const historyText = await historyResp.text().catch(() => "");

if (!historyResp.ok) {
  console.log("[gmail] history.list failed", {
    connectionId: conn.id,
    status: historyResp.status,
    body: historyText
  });
  return;
}

const historyJson = historyText ? JSON.parse(historyText) : {};
const historyRows = Array.isArray(historyJson.history) ? historyJson.history : [];

const messageIds = [
  ...new Set(
    historyRows.flatMap((h) =>
      Array.isArray(h.messagesAdded)
        ? h.messagesAdded
            .map((m) => m?.message?.id)
            .filter(Boolean)
        : []
    )
  )
];

console.log("[gmail] history.list success", {
  connectionId: conn.id,
  historyCount: historyRows.length,
  messageCount: messageIds.length,
  latestHistoryId: historyJson.historyId || null
});

if (!messageIds.length) {
  console.log("[gmail] no new message ids from history", {
    connectionId: conn.id,
    email
  });
  return;
}
  for (const messageId of messageIds) {

  console.log("[gmail] fetching message", {
    connectionId: conn.id,
    messageId
  });

  const msgResp = await fetch(
    `https://gmail.googleapis.com/gmail/v1/users/me/messages/${encodeURIComponent(messageId)}?format=metadata&metadataHeaders=Message-ID&metadataHeaders=Subject&metadataHeaders=From&metadataHeaders=In-Reply-To&metadataHeaders=References`,
    {
      method: "GET",
      headers: {
        Authorization: `Bearer ${accessToken}`,
        Accept: "application/json"
      }
    }
  );

  const msgText = await msgResp.text().catch(() => "");

  if (!msgResp.ok) {
    console.log("[gmail] message fetch failed", {
      connectionId: conn.id,
      messageId,
      status: msgResp.status,
      body: msgText
    });
    continue;
  }

  const msgJson = msgText ? JSON.parse(msgText) : null;

  const headers = msgJson?.payload?.headers || [];

  const headerMap = Object.fromEntries(
    headers.map(h => [h.name?.toLowerCase(), h.value])
  );

  const internetMessageId = headerMap["message-id"] || null;
  const subject = headerMap["subject"] || null;
  const from = headerMap["from"] || null;
  const inReplyTo = headerMap["in-reply-to"] || null;
  const references = headerMap["references"] || null;

  console.log("[gmail] parsed message headers", {
    messageId,
    internetMessageId,
    subject
  });

  if (!internetMessageId) {
  console.log("[gmail] missing internetMessageId -> skip", {
    messageId
  });
  continue;
}

/*
DEDUPLICATION
*/
let insertedNew = false;

try {
  const resp = await fetch(`${SUPABASE_URL}/rest/v1/email_webhook_dedupe`, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      apikey: SUPABASE_SERVICE_ROLE_KEY,
      Authorization: `Bearer ${SUPABASE_SERVICE_ROLE_KEY}`,
      Prefer: "resolution=ignore-duplicates,return=representation",
    },
    body: JSON.stringify([
      {
        provider: "google",
        connection_id: conn.id,
        subscription_id: "gmail",
        message_id: internetMessageId
      }
    ]),
  });

  if (!resp.ok) {
    const t = await resp.text().catch(() => "");
    console.log("[gmail] dedupe insert failed", resp.status, t);
    continue;
  }

  const rows = await resp.json().catch(() => []);
  insertedNew = Array.isArray(rows) && rows.length > 0;

} catch (e) {
  console.log("[gmail] dedupe insert exception", e?.message || e);
  continue;
}

if (!insertedNew) {
  console.log("[gmail] duplicate message -> skip", {
    internetMessageId
  });
  continue;
}

/*
INSERT INBOUND MESSAGE
*/

const senderEmail = from
  ? from.match(/<(.+?)>/)?.[1] || from
  : null;

await sbPost(
  "email_inbound_messages",
  [
    {
      user_id: conn.user_id,
      connection_id: conn.id,
      subscription_id: "gmail",
      provider: "google",
      ms_message_id: messageId,
      internet_message_id: internetMessageId,
      subject: subject || null,
      sender_email: senderEmail,
      received_at: new Date().toISOString(),
      body_preview: null,
      raw: msgJson
    }
  ],
  "return=minimal"
);

console.log("[gmail] inbound message inserted", {
  connectionId: conn.id,
  messageId,
  internetMessageId
});
}
}
// Next step will process this
const latestHistoryId = historyJson?.historyId || historyId;

if (latestHistoryId) {
  const currentHistoryId = conn.gmail_history_id || null;

  const shouldAdvance =
    !currentHistoryId ||
    BigInt(String(latestHistoryId)) > BigInt(String(currentHistoryId));

  if (shouldAdvance) {
    await sbPatch(
      `email_connections?id=eq.${encodeURIComponent(conn.id)}`,
      {
        gmail_history_id: String(latestHistoryId),
        gmail_last_push_at: new Date().toISOString()
      },
      "return=minimal"
    );

    console.log("[gmail] advanced gmail_history_id", {
      connectionId: conn.id,
      previous: currentHistoryId,
      next: String(latestHistoryId)
    });
  } else {
    await sbPatch(
      `email_connections?id=eq.${encodeURIComponent(conn.id)}`,
      {
        gmail_last_push_at: new Date().toISOString()
      },
      "return=minimal"
    );

    console.log("[gmail] gmail_history_id not advanced", {
      connectionId: conn.id,
      current: currentHistoryId,
      latestHistoryId: String(latestHistoryId)
    });
  }
}
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
