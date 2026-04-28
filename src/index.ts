import express from "express";
import {
  McpServer,
  ResourceTemplate,
} from "@modelcontextprotocol/sdk/server/mcp.js";
import { StreamableHTTPServerTransport } from "@modelcontextprotocol/sdk/server/streamableHttp.js";
import { z } from "zod";
// Import Express types correctly
import type { Request, Response } from "express";

// Enable debug logging to see what's happening
process.env.DEBUG = "mcp:*";

const app = express();

const server = new McpServer({
  name: "Echo",
  version: "1.0.0",
});

// Register our capabilities
server.resource(
  "echo",
  new ResourceTemplate("echo://{message}", { list: undefined }),
  async (uri, { message }) => ({
    contents: [
      {
        uri: uri.href,
        text: `Resource echo: ${message}`,
      },
    ],
  }),
);

server.tool("echo", { message: z.string() }, async ({ message }) => ({
  content: [{ type: "text", text: `Tool echo: ${message}` }],
}));

server.prompt("echo", { message: z.string() }, ({ message }) => ({
  messages: [
    {
      role: "user",
      content: {
        type: "text",
        text: `Please process this message: ${message}`,
      },
    },
  ],
}));

app.post("/mcp", async (req: Request, res: Response) => {
  try {
    const transport = new StreamableHTTPServerTransport({
      sessionIdGenerator: undefined,
    });

    res.on("close", () => {
      console.log("Request closed");
      transport.close();
    });

    await server.connect(transport);
    await transport.handleRequest(req, res);
  } catch (error) {
    console.error("Error handling MCP request:", error);
    if (!res.headersSent) {
      res.status(500).json({
        jsonrpc: "2.0",
        error: {
          code: -32603,
          message: "Internal server error",
        },
        id: null,
      });
    }
  }
});

app.get("/mcp", async (req: Request, res: Response) => {
  console.log("Received GET MCP request");
  res.writeHead(405).end(
    JSON.stringify({
      jsonrpc: "2.0",
      error: {
        code: -32000,
        message:
          "Method not allowed. Use POST to interact with the MCP server. Follow README for details.",
      },
      id: null,
    }),
  );
});

app.delete("/mcp", async (req: Request, res: Response) => {
  console.log("Received DELETE MCP request");
  res.writeHead(405).end(
    JSON.stringify({
      jsonrpc: "2.0",
      error: {
        code: -32000,
        message:
          "Method not allowed. Use POST to interact with the MCP server. Follow README for details.",
      },
      id: null,
    }),
  );
});

// Start the server
const PORT = process.env.MCP_SERVER_PORT || 8080;
app.listen(PORT, () => {
  console.log(`MCP Stateless Streamable HTTP Server listening on port ${PORT}`);
});

// Base URL for the credit-store API
const API_URL =
  process.env.MCP_API_URL || "https://credit-store-production.up.railway.app";

// Helper function for making API requests
async function makeAPIRequest<T>(
  url: string,
  method: string,
  body?: any,
  headers?: HeadersInit,
): Promise<T | null> {
  const defaultHeaders = {
    "Content-Type": "application/json",
    ...headers,
  };

  try {
    const response = await fetch(url, {
      method,
      headers: defaultHeaders,
      body: body ? JSON.stringify(body) : undefined,
    });
    if (!response.ok) {
      throw new Error(`HTTP error! status: ${response.status}`);
    }
    return (await response.json()) as T;
  } catch (error) {
    console.error("Error making API request:", error);
    return null;
  }
}

// Interfaces for request and response types
interface UserBalance {
  userId: string;
  balance: number;
}

interface CreditPack {
  id: string;
  name: string;
  price: number;
  credits: number;
}

interface LedgerEntry {
  date: string;
  amount: number;
  description: string;
}

interface Settings {
  featureXEnabled: boolean;
  maxCreditsPerUser: number;
}

// Register MCP tools

// @ts-ignore
server.tool(
  "post-mcp",
  "MCP endpoint for credit operations using x-user-id header",
  {
    userId: z.string().describe("User ID for credit operations"),
    operation: z
      .object({
        type: z.string(),
        amount: z.number(),
      })
      .describe("Operation details"),
  },
  async ({ userId, operation }) => {
    const url = `${API_URL}/mcp`;
    const result = await makeAPIRequest<any>(
      url,
      "POST",
      { userId, operation },
      { "x-user-id": userId },
    );

    if (!result) {
      return {
        content: [{ type: "text", text: "Failed to perform credit operation" }],
      };
    }

    return { content: [{ type: "text", text: "Credit operation successful" }] };
  },
);

// @ts-ignore
server.tool(
  "post-mcp-admin",
  "Admin MCP endpoint for credit operations",
  {
    operation: z
      .object({
        type: z.string(),
        amount: z.number(),
        userId: z.string(),
      })
      .describe("Admin operation details"),
  },
  async ({ operation }) => {
    const url = `${API_URL}/mcp/admin`;
    const result = await makeAPIRequest<any>(url, "POST", operation);

    if (!result) {
      return {
        content: [
          { type: "text", text: "Failed to perform admin credit operation" },
        ],
      };
    }

    return {
      content: [{ type: "text", text: "Admin credit operation successful" }],
    };
  },
);

// @ts-ignore
server.tool(
  "get-balance-by-user-id",
  "Get user balance using userId path parameter",
  {
    userId: z.string().describe("User ID to retrieve balance"),
  },
  async ({ userId }) => {
    const url = `${API_URL}/u/${userId}`;
    const balanceData = await makeAPIRequest<UserBalance>(url, "GET");

    if (!balanceData) {
      return {
        content: [{ type: "text", text: "Failed to retrieve user balance" }],
      };
    }

    return {
      content: [
        {
          type: "text",
          text: `User balance for ${userId}: ${balanceData.balance}`,
        },
      ],
    };
  },
);

// @ts-ignore
server.tool(
  "purchase-credit-pack",
  "Purchase credit pack for user",
  {
    userId: z.string().describe("User ID for purchasing credit pack"),
    packId: z.string().describe("Credit pack ID to purchase"),
  },
  async ({ userId, packId }) => {
    const url = `${API_URL}/u/${userId}`;
    const result = await makeAPIRequest<any>(url, "POST", { packId });

    if (!result) {
      return {
        content: [{ type: "text", text: "Failed to purchase credit pack" }],
      };
    }

    return {
      content: [{ type: "text", text: "Credit pack purchased successfully" }],
    };
  },
);

// @ts-ignore
server.tool(
  "get-ledger-entries",
  "Retrieve ledger entries for a user",
  {
    userId: z.string().describe("User ID to retrieve ledger entries"),
  },
  async ({ userId }) => {
    const url = `${API_URL}/u/${userId}/ledger`;
    const ledgerData = await makeAPIRequest<LedgerEntry[]>(url, "GET");

    if (!ledgerData) {
      return {
        content: [{ type: "text", text: "Failed to retrieve ledger entries" }],
      };
    }

    const entriesText = ledgerData
      .map(
        (entry) =>
          `Date: ${entry.date}, Amount: ${entry.amount}, Description: ${entry.description}`,
      )
      .join("\n");
    return {
      content: [
        { type: "text", text: `Ledger entries for ${userId}:\n${entriesText}` },
      ],
    };
  },
);

// @ts-ignore
server.tool(
  "get-credit-packs",
  "Retrieve all available credit packs",
  {},
  async () => {
    const url = `${API_URL}/packs`;
    const packsData = await makeAPIRequest<CreditPack[]>(url, "GET");

    if (!packsData) {
      return {
        content: [{ type: "text", text: "Failed to retrieve credit packs" }],
      };
    }

    const packsText = packsData
      .map(
        (pack) =>
          `ID: ${pack.id}, Name: ${pack.name}, Price: ${pack.price}, Credits: ${pack.credits}`,
      )
      .join("\n");
    return {
      content: [
        { type: "text", text: `Available credit packs:\n${packsText}` },
      ],
    };
  },
);

// @ts-ignore
server.tool("get-health", "Health check endpoint", {}, async () => {
  const url = `${API_URL}/health`;
  const healthData = await makeAPIRequest<any>(url, "GET");

  if (!healthData) {
    return { content: [{ type: "text", text: "Health check failed" }] };
  }

  return { content: [{ type: "text", text: "API is healthy" }] };
});

// @ts-ignore
server.tool(
  "get-settings",
  "Retrieve all settings for the credit system and features",
  {},
  async () => {
    const url = `${API_URL}/settings`;
    const settingsData = await makeAPIRequest<Settings>(url, "GET");

    if (!settingsData) {
      return {
        content: [{ type: "text", text: "Failed to retrieve settings" }],
      };
    }

    return {
      content: [
        { type: "text", text: `Settings: ${JSON.stringify(settingsData)}` },
      ],
    };
  },
);

// @ts-ignore
server.tool(
  "update-settings",
  "Update settings for the credit system and features",
  {
    settings: z
      .object({
        featureXEnabled: z.boolean(),
        maxCreditsPerUser: z.number(),
      })
      .describe("Settings to update"),
  },
  async ({ settings }) => {
    const url = `${API_URL}/settings`;
    const result = await makeAPIRequest<any>(url, "PUT", settings);

    if (!result) {
      return { content: [{ type: "text", text: "Failed to update settings" }] };
    }

    return {
      content: [{ type: "text", text: "Settings updated successfully" }],
    };
  },
);
