import * as appConfig from "../config.js";

export type ErpCredentials = { apiKey: string; apiSecret: string };

export class ErpError extends Error {
  constructor(
    message: string,
    public readonly status: number,
    public readonly body?: unknown
  ) {
    super(message);
    this.name = "ErpError";
  }
}

function authHeader(c: ErpCredentials): string {
  return `token ${c.apiKey}:${c.apiSecret}`;
}

async function parseJson(res: Response): Promise<unknown> {
  const text = await res.text();
  if (!text) return undefined;
  try {
    return JSON.parse(text) as unknown;
  } catch {
    return text;
  }
}

export class ErpNextClient {
  constructor(private readonly baseUrl: string) {}

  private async request(
    method: string,
    path: string,
    creds: ErpCredentials,
    opts?: { body?: unknown; form?: FormData; headers?: Record<string, string> }
  ): Promise<unknown> {
    const url = `${this.baseUrl}${path.startsWith("/") ? "" : "/"}${path}`;
    const headers: Record<string, string> = {
      Authorization: authHeader(creds),
      Accept: "application/json",
      ...opts?.headers,
    };
    if (appConfig.ERP_SITE_NAME) {
      headers["X-Frappe-Site-Name"] = appConfig.ERP_SITE_NAME;
    }
    if (opts?.body !== undefined && !opts.form) {
      headers["Content-Type"] = "application/json";
    }
    if (opts?.form) {
      delete headers["Content-Type"];
    }
    const res = await fetch(url, {
      method,
      headers,
      body: opts?.form ?? (opts?.body !== undefined ? JSON.stringify(opts.body) : undefined),
    });
    const data = await parseJson(res);
    if (!res.ok) {
      throw new ErpError(`Upstream HTTP ${res.status}`, res.status, data);
    }
    return data;
  }

  /** GET /api/resource/:doctype/:name */
  async getDoc(creds: ErpCredentials, doctype: string, name: string): Promise<Record<string, unknown>> {
    const enc = encodeURIComponent;
    const path = `/api/resource/${enc(doctype)}/${enc(name)}`;
    const data = (await this.request("GET", path, creds)) as { data?: Record<string, unknown> };
    return data.data ?? {};
  }

  /** GET /api/resource/:doctype with filters/fields (Frappe JSON query params). */
  async listDocs(
    creds: ErpCredentials,
    doctype: string,
    opts: {
      filters?: unknown[];
      fields?: string[];
      order_by?: string;
      limit_start?: number;
      limit_page_length?: number;
    }
  ): Promise<{ data?: unknown[] }> {
    const enc = encodeURIComponent;
    const q = new URLSearchParams();
    if (opts.filters) q.set("filters", JSON.stringify(opts.filters));
    if (opts.fields) q.set("fields", JSON.stringify(opts.fields));
    if (opts.order_by) q.set("order_by", opts.order_by);
    if (opts.limit_start != null) q.set("limit_start", String(opts.limit_start));
    if (opts.limit_page_length != null) q.set("limit_page_length", String(opts.limit_page_length));
    const path = `/api/resource/${enc(doctype)}?${q.toString()}`;
    return this.request("GET", path, creds) as Promise<{ data?: unknown[] }>;
  }

  /** POST /api/resource/:doctype */
  async createDoc(creds: ErpCredentials, doctype: string, doc: Record<string, unknown>): Promise<Record<string, unknown>> {
    const enc = encodeURIComponent;
    const path = `/api/resource/${enc(doctype)}`;
    const data = (await this.request("POST", path, creds, { body: { data: doc } })) as {
      data?: Record<string, unknown>;
    };
    return data.data ?? {};
  }

  /** PUT /api/resource/:doctype/:name */
  async updateDoc(
    creds: ErpCredentials,
    doctype: string,
    name: string,
    doc: Record<string, unknown>
  ): Promise<Record<string, unknown>> {
    const enc = encodeURIComponent;
    const path = `/api/resource/${enc(doctype)}/${enc(name)}`;
    const data = (await this.request("PUT", path, creds, { body: { data: doc } })) as {
      data?: Record<string, unknown>;
    };
    return data.data ?? {};
  }

  /** POST /api/method/:method */
  async callMethod(
    creds: ErpCredentials,
    method: string,
    args?: Record<string, unknown>
  ): Promise<unknown> {
    const path = `/api/method/${method}`;
    return this.request("POST", path, creds, { body: args ?? {} });
  }

  /**
   * Prefer over GET /api/resource for some sites (avoids occasional 500s on complex filters).
   * Returns rows from `frappe.client.get_list` (`message` array).
   */
  async getList(
    creds: ErpCredentials,
    doctype: string,
    opts: {
      filters?: unknown[];
      fields?: string[];
      order_by?: string;
      limit_page_length?: number;
    }
  ): Promise<unknown[]> {
    const payload: Record<string, unknown> = {
      doctype,
      fields: opts.fields ?? ["name"],
      filters: opts.filters ?? [],
      limit_page_length: opts.limit_page_length ?? 20,
    };
    if (opts.order_by) payload.order_by = opts.order_by;
    const data = (await this.callMethod(creds, "frappe.client.get_list", payload)) as {
      message?: unknown[];
    };
    const msg = data?.message;
    return Array.isArray(msg) ? msg : [];
  }

  /** Submit document (workflow/submit button equivalent). */
  async submitDoc(creds: ErpCredentials, doctype: string, name: string): Promise<unknown> {
    return this.callMethod(creds, "frappe.client.submit", {
      doc: { doctype, name },
    });
  }

  /** Cancel submitted document (recall / void — ERP rules still apply). */
  async cancelDoc(creds: ErpCredentials, doctype: string, name: string): Promise<unknown> {
    return this.callMethod(creds, "frappe.client.cancel", {
      doctype,
      name,
    });
  }

  /** Multipart upload to Frappe */
  async uploadFile(
    creds: ErpCredentials,
    params: {
      buffer: Buffer;
      filename: string;
      contentType: string;
      isPrivate?: boolean;
      doctype: string;
      docname: string;
      fieldname?: string;
    }
  ): Promise<unknown> {
    const form = new FormData();
    form.append(
      "file",
      new Blob([new Uint8Array(params.buffer)], { type: params.contentType }),
      params.filename
    );
    form.append("is_private", params.isPrivate ? "1" : "0");
    form.append("doctype", params.doctype);
    form.append("docname", params.docname);
    if (params.fieldname) form.append("fieldname", params.fieldname);

    const path = `/api/method/upload_file`;
    return this.request("POST", path, creds, { form });
  }
}

export function defaultClient(): ErpNextClient {
  return new ErpNextClient(appConfig.ERP_BASE_URL);
}
