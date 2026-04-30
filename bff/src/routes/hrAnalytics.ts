import type { FastifyPluginAsync } from "fastify";
import { defaultClient, ErpError } from "../erpnext/client.js";
import { resolveHrContext, HttpError } from "../context/resolveHrContext.js";

const erp = defaultClient();

function ymd(input: unknown): string {
  const s = String(input ?? "").trim();
  return /^\d{4}-\d{2}-\d{2}$/.test(s) ? s : "";
}

function toDateOrDefault(s: string, fallback: Date): Date {
  if (!s) return fallback;
  const d = new Date(`${s}T00:00:00Z`);
  return Number.isNaN(d.getTime()) ? fallback : d;
}

function daysBetweenInclusive(from: Date, to: Date): number {
  const ms = to.getTime() - from.getTime();
  return Math.max(1, Math.floor(ms / (24 * 60 * 60 * 1000)) + 1);
}

function monthBounds(offset = 0): { start: string; end: string } {
  const now = new Date();
  const first = new Date(Date.UTC(now.getUTCFullYear(), now.getUTCMonth() + offset, 1));
  const last = new Date(Date.UTC(now.getUTCFullYear(), now.getUTCMonth() + offset + 1, 0));
  return {
    start: first.toISOString().slice(0, 10),
    end: last.toISOString().slice(0, 10),
  };
}

function chunk<T>(arr: T[], size: number): T[][] {
  const out: T[][] = [];
  for (let i = 0; i < arr.length; i += size) out.push(arr.slice(i, i + size));
  return out;
}

export const hrAnalyticsRoutes: FastifyPluginAsync = async (app) => {
  app.get("/v1/hr-analytics/overview", async (req, reply) => {
    let ctx;
    try {
      ctx = resolveHrContext(req);
    } catch (e) {
      if (e instanceof HttpError) return reply.status(e.status).send({ error: e.message });
      throw e;
    }
    if (!ctx.canSubmitOnBehalf) return reply.status(403).send({ error: "HR admin privileges required" });

    const q = (req.query ?? {}) as Record<string, unknown>;
    const fromRaw = ymd(q.from_date ?? q.from);
    const toRaw = ymd(q.to_date ?? q.to);
    const now = new Date();
    const from = toDateOrDefault(fromRaw, new Date(Date.UTC(now.getUTCFullYear(), now.getUTCMonth(), 1)));
    const to = toDateOrDefault(toRaw, now);
    const fromYmd = from.toISOString().slice(0, 10);
    const toYmd = to.toISOString().slice(0, 10);
    const windowDays = daysBetweenInclusive(from, to);

    try {
      const employees = await erp.getList(ctx.creds, "Employee", {
        filters: [["company", "=", ctx.company]],
        fields: ["name", "status", "relieving_date", "date_of_joining"],
        limit_page_length: 5000,
      });
      const employeeRows = employees as Record<string, unknown>[];
      const totalHeadcount = employeeRows.length;
      const activeHeadcount = employeeRows.filter((e) => String(e.status ?? "").toLowerCase() === "active").length;
      const exitsInWindow = employeeRows.filter((e) => {
        const d = String(e.relieving_date ?? "").slice(0, 10);
        return d && d >= fromYmd && d <= toYmd;
      }).length;
      const avgHeadcount = Math.max(1, (totalHeadcount + activeHeadcount) / 2);
      const attritionRatePct = Number(((exitsInWindow / avgHeadcount) * 100).toFixed(2));

      const leaveRows = await erp.getList(ctx.creds, "Leave Application", {
        filters: [
          ["company", "=", ctx.company],
          ["docstatus", "=", 1],
          ["status", "=", "Approved"],
          ["from_date", ">=", fromYmd],
          ["to_date", "<=", toYmd],
        ],
        fields: ["name", "total_leave_days"],
        limit_page_length: 5000,
      });
      const approvedLeaveDays = (leaveRows as Record<string, unknown>[]).reduce(
        (sum, row) => sum + (Number(row.total_leave_days ?? 0) || 0),
        0
      );
      const leaveUtilizationPct = Number(
        ((approvedLeaveDays / Math.max(1, activeHeadcount * windowDays)) * 100).toFixed(2)
      );

      const openings = await erp.getList(ctx.creds, "Job Opening", {
        filters: [
          ["company", "=", ctx.company],
          ["status", "=", "Open"],
        ],
        fields: ["name"],
        limit_page_length: 500,
      });
      const openingNames = (openings as Record<string, unknown>[])
        .map((r) => String(r.name ?? "").trim())
        .filter(Boolean);

      let applicantCount = 0;
      if (openingNames.length > 0) {
        for (const part of chunk(openingNames, 80)) {
          const apps = await erp.getList(ctx.creds, "Job Applicant", {
            filters: [["job_title", "in", part]],
            fields: ["name"],
            limit_page_length: 1000,
          });
          applicantCount += (apps as unknown[]).length;
        }
      }
      const openRoles = openingNames.length;

      const currentMonth = monthBounds(0);
      const previousMonth = monthBounds(-1);
      const [slipsCurrent, slipsPrevious] = await Promise.all([
        erp.getList(ctx.creds, "Salary Slip", {
          filters: [
            ["company", "=", ctx.company],
            ["docstatus", "=", 1],
            ["start_date", ">=", currentMonth.start],
            ["end_date", "<=", currentMonth.end],
          ],
          fields: ["name", "net_pay"],
          limit_page_length: 5000,
        }),
        erp.getList(ctx.creds, "Salary Slip", {
          filters: [
            ["company", "=", ctx.company],
            ["docstatus", "=", 1],
            ["start_date", ">=", previousMonth.start],
            ["end_date", "<=", previousMonth.end],
          ],
          fields: ["name", "net_pay"],
          limit_page_length: 5000,
        }),
      ]);
      const currNet = (slipsCurrent as Record<string, unknown>[]).reduce((sum, r) => sum + (Number(r.net_pay ?? 0) || 0), 0);
      const prevNet = (slipsPrevious as Record<string, unknown>[]).reduce((sum, r) => sum + (Number(r.net_pay ?? 0) || 0), 0);
      const payrollVariancePct = prevNet > 0 ? Number((((currNet - prevNet) / prevNet) * 100).toFixed(2)) : null;

      return {
        data: {
          window: { from_date: fromYmd, to_date: toYmd, days: windowDays },
          workforce: {
            total_headcount: totalHeadcount,
            active_headcount: activeHeadcount,
            exits_in_window: exitsInWindow,
            attrition_rate_pct: attritionRatePct,
          },
          leave: {
            approved_leave_days: Number(approvedLeaveDays.toFixed(2)),
            utilization_pct: leaveUtilizationPct,
          },
          recruitment: {
            open_roles: openRoles,
            applicants_in_pipeline: applicantCount,
          },
          payroll: {
            net_pay_current_month: Number(currNet.toFixed(2)),
            net_pay_previous_month: Number(prevNet.toFixed(2)),
            variance_pct: payrollVariancePct,
          },
        },
      };
    } catch (e) {
      if (e instanceof ErpError) {
        return reply.status(e.status >= 500 ? 502 : e.status).send({ error: String(e.message) });
      }
      throw e;
    }
  });
};

