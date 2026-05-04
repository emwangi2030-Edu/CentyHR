import {
  type BulkPaymentBatch, type InsertBulkPaymentBatch,
  type BulkPaymentItem, type InsertBulkPaymentItem, type B2cDispatchQueueRow,
  type CashAdvance, type InsertCashAdvance,
  type Investment, type InsertInvestment,
  type Transaction, type InsertTransaction,
  type SavedTemplate, type InsertSavedTemplate,
  type RecurringPayment, type InsertRecurringPayment,
  type PaymentLink, type InsertPaymentLink,
  type WalletTopup, type InsertWalletTopup,
  type QuickPayRecipient, type InsertQuickPayRecipient,
  type User, type Business, type InsertBusiness, type Session, type Approval, type InsertApproval, type UserBusinessMembership,
  type InvestorProfile, type InsertInvestorProfile,
  type Notification, type InsertNotification,
  type MpesaAuditLog,
  type MpesaTransaction,
  type Subscription, type InsertSubscription,
  type Invoice, type InsertInvoice,
  type ArCustomer, type InsertArCustomer,
  type ArInvoice, type InsertArInvoice,
  type ArInvoiceLine, type InsertArInvoiceLine,
  type ArPayment, type InsertArPayment,
  type ArAllocation, type InsertArAllocation,
  type ArStatementEntry, type InsertArStatementEntry,
  type SupportTicket, type InsertSupportTicket,
  type DemoRequest, type InsertDemoRequest,
  type TicketComment, type InsertTicketComment,
  type OverdraftLimit, type InsertOverdraftLimit,
  type PlatformSetting,
  type BusinessPaymentSettings, type InsertBusinessPaymentSettings,
  type PaymentFavorite, type InsertPaymentFavorite,
  type HakikishaLookup, type InsertHakikishaLookup,
  type CentypackMirrorSyncLog,
  type HrEmployeeDraft,
  type HrEmployeeInvite,
  type HrOrgUnit,
  type HrLifecycleEvent,
  type HrOnboardingTemplate,
  type HrOnboardingTask,
  type Team,
  type InsertTeam,
  type TeamMember,
  type EmployeeWalletAccount,
  type InsertEmployeeWalletAccount,
  type EmployeeWalletLedgerEntry,
  type InsertEmployeeWalletLedgerEntry,
  type LaborWorker, type InsertLaborWorker,
  type LaborPieceRate, type InsertLaborPieceRate,
  type LaborDailyEntry, type InsertLaborDailyEntry,
  type LaborPayrollRun, type InsertLaborPayrollRun,
  type LaborPayrollLine, type InsertLaborPayrollLine,
  type ImportedTransaction, type InsertImportedTransaction,
  bulkPaymentBatches, bulkPaymentItems, cashAdvances, investments, transactions, savedTemplates, recurringPayments, paymentLinks,
  walletTopups, quickPayRecipients, investorProfiles, notifications, mpesaAuditLogs, mpesaTransactions,
  hakikishaLookups,
  centypackMirrorSyncLogs,
  users, businesses, sessions, approvals, overdraftLimits, platformSettings,
  businessPaymentSettings, paymentFavorites,
  subscriptions, invoices, supportTickets, ticketComments, demoRequests,
  arCustomers, arInvoices, arInvoiceLines, arPayments, arAllocations, arStatementEntries,
  hrErpCredentials,
  centyposErpCredentials,
  hrEmployeeDrafts,
  hrEmployeeInvites,
  hrInfoRequests,
  hrOrgUnits,
  hrLifecycleEvents,
  hrOnboardingTemplates,
  hrOnboardingTasks,
  teams,
  teamMembers,
  employeeWalletAccounts,
  employeeWalletLedgerEntries,
  laborWorkers, laborPieceRates, laborDailyEntries, laborPayrollRuns, laborPayrollLines,
  importedTransactions,
  userPlatformRoles, userBusinessMemberships, salaryAdvanceApprovals,
  leavePolicies, leaveBalances, leaveApplications, compulsoryLeaves, employeeUserLinks,
  leaveForfeitureLogs, leaveWarningLogs, overtimePolicies, centypackFarmers, centypackCrops, centypackVarieties, centypackCartonTypes, centypackWorkerCategories, centypackWarehouses, centypackGradeCodes, centypackDefectTypes, centypackIntake, centypackGdns, centypackGdnItems, centypackGradingSessions, centypackGradingLines, centypackGradingDefects, centypackPackSessions, centypackPackLines, centypackLabourers, centypackLabourAttendance, centypackPackLineLabourers, centypackStockLedger,
  type LeavePolicy, type InsertLeavePolicy,
  type LeaveBalance, type InsertLeaveBalance,
  type LeaveApplication, type InsertLeaveApplication,
  type CompulsoryLeave, type InsertCompulsoryLeave,
  type LeaveForfeitureLog, type InsertLeaveForfeitureLog,
  type LeaveWarningLog, type InsertLeaveWarningLog,
  type OvertimePolicy, type InsertOvertimePolicy,
  type CentypackFarmer, type InsertCentypackFarmer,
  type CentypackCrop, type InsertCentypackCrop,
  type CentypackVariety, type InsertCentypackVariety,
  type CentypackCartonType, type InsertCentypackCartonType,
  type CentypackWorkerCategory, type InsertCentypackWorkerCategory,
  type CentypackWarehouse, type InsertCentypackWarehouse,
  type CentypackGradeCode, type InsertCentypackGradeCode,
  type CentypackDefectType, type InsertCentypackDefectType,
  type CentypackIntake, type InsertCentypackIntake,
  type CentypackGdn, type InsertCentypackGdn,
  type CentypackGdnItem, type InsertCentypackGdnItem,
  type CentypackGradingSession, type InsertCentypackGradingSession,
  type CentypackGradingLine, type InsertCentypackGradingLine,
  type CentypackGradingDefect, type InsertCentypackGradingDefect,
  type CentypackPackSession, type InsertCentypackPackSession,
  type CentypackPackLine, type InsertCentypackPackLine,
  type CentypackLabourer, type InsertCentypackLabourer,
  type CentypackLabourAttendance, type InsertCentypackLabourAttendance,
  type CentypackPackLineLabourer,
  type CentypackStockLedger, type InsertCentypackStockLedger,
} from "@shared/schema";
import { desc, asc, eq, sql, and, gt, gte, lte, lt, inArray, or, like, ilike, isNull, isNotNull, type SQL } from "drizzle-orm";
import bcrypt from "bcryptjs";
import crypto from "crypto";
import type { PoolClient } from "pg";
import { db, pool, withRetry } from "./db";

function isMissingRelation(err: unknown, relationName: string): boolean {
  const msg = String((err as { message?: string })?.message ?? "");
  return msg.includes(relationName) && msg.includes("does not exist");
}

export type EnrichedAdminTransaction = Transaction & {
  actorName: string | null;
  actorUserId: string | null;
  actorEmail: string | null;
  businessName: string | null;
  businessId: string | null;
};

export type ExpenseJournalRow = {
  itemId: string;
  batchId: string;
  recipient: string;
  amount: string;
  fee: string;
  reference: string | null;
  status: string;
  expenseCategory: string;  // never null in API response — derived if not stored
  processedAt: Date | null;
  batchCreatedAt: Date;
  batchType: string;
  paymentType: string;
  createdByName: string | null;
  mpesaTransactionId: string | null;
  systemRef: string | null;
};

export interface PaginatedResult<T> {
  data: T[];
  total: number;
  page: number;
  pageSize: number;
  totalPages: number;
}

export type CompanyWalletRow = {
  businessId: string;
  businessName: string;
  kycStatus: string;
  accountNumber: string | null;
  isSandbox: boolean;
  createdAt: Date;
  totalTopups: number;
  totalLinkInflows: number;
  totalDisbursements: number;
  pendingDisbursements: number;
  currentBalance: number;
};

export type StockOnHandRow = {
  cropId: string;
  cropName: string;
  varietyId: string | null;
  varietyName: string | null;
  gradeCodeId: string | null;
  gradeCodeName: string | null;
  cartonTypeId: string | null;
  cartonTypeName: string | null;
  warehouseId: string | null;
  warehouseName: string | null;
  stage: string;
  qtyKgBalance: number;
  cartonBalance: number;
};

export type StockMovementRow = CentypackStockLedger & {
  cropName: string;
  varietyName: string | null;
  gradeCodeName: string | null;
  cartonTypeName: string | null;
  warehouseName: string | null;
};

export interface IStorage {
  // Auth
  createUser(
    email: string,
    password: string,
    fullName: string,
    phone: string | null,
    role: string,
    createdBy?: string,
    accountType?: string,
    businessId?: string,
    status?: string,
    emailVerifiedAt?: Date | null,
  ): Promise<User>;
  createUserWithGoogle(googleId: string, email: string, fullName: string, profileImageUrl?: string | null): Promise<User>;
  getUserByEmail(email: string): Promise<User | undefined>;
  getUserByGoogleId(googleId: string): Promise<User | undefined>;
  getUserById(id: string): Promise<User | undefined>;
  getUsersByIds(ids: string[]): Promise<User[]>;
  getUsers(): Promise<User[]>;
  getUsersByBusinessId(businessId: string): Promise<User[]>;
  listUserBusinessMemberships(userId: string): Promise<Business[]>;
  /** Businesses the user can open, with effective role in each (primary, membership, or owner). */
  listAccessibleBusinessesWithRoles(userId: string): Promise<Array<Business & { accessRole: string }>>;
  addUserToBusiness(
    userId: string,
    businessId: string,
    role?: string,
    opts?: { invitePending?: boolean },
  ): Promise<UserBusinessMembership>;
  getMembershipForBusiness(
    userId: string,
    businessId: string,
  ): Promise<{ role: string; status: string; inviteToken: string | null } | null>;
  acceptCompanyInvite(token: string, userId: string): Promise<{ ok: true } | { ok: false; message: string }>;
  getCompanyInvitePreviewByToken(token: string): Promise<{ companyName: string; role: string } | null>;
  getUserBusinessMembershipRole(userId: string, businessId: string): Promise<string | null>;
  setUserBusinessMembershipRole(userId: string, businessId: string, role: string): Promise<void>;
  userHasBusinessAccess(userId: string, businessId: string): Promise<boolean>;
  getUsersByRole(role: string): Promise<User[]>;
  updateUserRole(id: string, role: string): Promise<User | undefined>;
  updateUserStatus(id: string, status: string): Promise<User | undefined>;
  updateUserBusinessId(id: string, businessId: string): Promise<User | undefined>;
  updateUserProfileImage(id: string, profileImageUrl: string | null): Promise<User | undefined>;
  updateUserProfile(id: string, data: { fullName?: string; phone?: string | null }): Promise<User | undefined>;
  updateUserGoogleId(id: string, googleId: string): Promise<User | undefined>;
  setEmailVerificationToken(userId: string, token: string, expiresAt: Date): Promise<User | undefined>;
  getUserByEmailVerificationToken(token: string): Promise<User | undefined>;
  markUserEmailVerified(userId: string): Promise<User | undefined>;
  verifyPassword(password: string, hash: string): Promise<boolean>;
  setPasswordResetToken(email: string, token: string, expiresAt: Date): Promise<User | undefined>;
  getUserByResetToken(token: string): Promise<User | undefined>;
  updateUserPassword(userId: string, newPassword: string): Promise<User | undefined>;
  updateUserEmail(userId: string, newEmail: string): Promise<User | undefined>;

  // Sessions
  createSession(userId: string): Promise<Session>;
  getSessionByToken(token: string): Promise<Session | undefined>;
  extendSession(token: string): Promise<Session | undefined>;
  deleteSession(token: string): Promise<void>;
  deleteUserSessions(userId: string): Promise<void>;
  setImpersonatedBusiness(token: string, businessId: string | null): Promise<Session | undefined>;
  setImpersonatedUser(token: string, userId: string | null): Promise<Session | undefined>;

  // Business KYC
  createBusiness(business: InsertBusiness): Promise<Business>;
  getBusinessByUserId(userId: string): Promise<Business | undefined>;
  /** Per-user Frappe API keys for HR / ERPNext (optional). */
  getHrErpCredentials(userId: string): Promise<{
    frappeApiKey: string;
    frappeApiSecret: string;
    companyOverride: string | null;
  } | null>;
  getCentyposErpCredentials(userId: string): Promise<{
    frappeApiKey: string;
    frappeApiSecret: string;
    companyOverride: string | null;
  } | null>;
  upsertCentyposErpCredentials(
    userId: string,
    frappeApiKey: string,
    frappeApiSecret: string,
    companyOverride?: string | null,
  ): Promise<void>;
  deleteCentyposErpCredentials(userId: string): Promise<void>;
  createHrEmployeeDraft(
    userId: string,
    payload: Record<string, unknown>,
    opts?: { status?: string }
  ): Promise<HrEmployeeDraft>;
  getHrEmployeeDraftById(id: string): Promise<HrEmployeeDraft | undefined>;
  claimNextHrEmployeeDraft(): Promise<HrEmployeeDraft | null>;
  completeHrEmployeeDraft(id: string, erpEmployeeName: string): Promise<void>;
  failHrEmployeeDraft(id: string, error: string): Promise<void>;
  createHrEmployeeInvite(params: {
    email: string;
    companyKey: string;
    invitedByEmail: string;
    expiresInDays?: number;
  }): Promise<HrEmployeeInvite>;
  getHrEmployeeInviteByToken(token: string): Promise<HrEmployeeInvite | undefined>;
  markHrEmployeeInviteCompleted(token: string): Promise<void>;
  createHrInfoRequest(params: {
    employeeId: string;
    employeeName: string;
    employeeEmail: string;
    adminEmail: string;
    adminUserId: string;
    companyKey: string;
    requestedFields: { fieldname: string; label: string }[];
    expiresInDays?: number;
  }): Promise<typeof hrInfoRequests.$inferSelect>;
  getHrInfoRequestByToken(token: string): Promise<typeof hrInfoRequests.$inferSelect | undefined>;
  updateHrInfoRequestSubmittedValues(token: string, submitted: Record<string, string>): Promise<void>;
  markHrInfoRequestSubmitted(token: string): Promise<void>;
  markHrInfoRequestCompleted(token: string): Promise<void>;
  getLatestCompletedHrInfoRequestByEmployee(employeeId: string): Promise<typeof hrInfoRequests.$inferSelect | undefined>;
  listHrInfoRequestsByCompany(companyKey: string, status?: string): Promise<(typeof hrInfoRequests.$inferSelect)[]>;
  getHrInfoRequestById(id: string): Promise<typeof hrInfoRequests.$inferSelect | undefined>;
  reviewHrInfoRequestById(
    id: string,
    status: "approved" | "rejected",
    remarks: string,
    reviewedByEmail: string,
  ): Promise<typeof hrInfoRequests.$inferSelect | undefined>;
  deleteHrOrgUnit(businessId: string, kind: "department" | "designation" | "branch", value: string): Promise<boolean>;
  renameHrOrgUnit(businessId: string, kind: "department" | "designation" | "branch", oldValue: string, newValue: string): Promise<HrOrgUnit | null>;
  listHrOrgUnits(
    businessId: string
  ): Promise<{ department: HrOrgUnit[]; designation: HrOrgUnit[]; branch: HrOrgUnit[] }>;
  upsertHrOrgUnit(
    businessId: string,
    kind: "department" | "designation" | "branch",
    value: string,
    userId: string
  ): Promise<HrOrgUnit>;
  listHrLifecycleEvents(businessId: string, employeeId: string): Promise<HrLifecycleEvent[]>;
  getHrLifecycleEventById(id: string): Promise<HrLifecycleEvent | undefined>;
  updateHrLifecycleEventStatus(
    id: string,
    status: "draft" | "submitted" | "approved" | "rejected" | "completed",
    notes?: string | null
  ): Promise<HrLifecycleEvent | undefined>;
  createHrLifecycleEvent(input: {
    businessId: string;
    employeeId: string;
    eventType: "promotion" | "transfer" | "offboarding_checklist" | "exit_interview";
    effectiveDate?: string | null;
    status?: string | null;
    notes?: string | null;
    payload?: Record<string, unknown>;
    userId: string;
  }): Promise<HrLifecycleEvent>;
  listHrOnboardingTemplates(
    businessId: string,
    filters?: { department?: string; designation?: string; activeOnly?: boolean }
  ): Promise<HrOnboardingTemplate[]>;
  createHrOnboardingTemplate(input: {
    businessId: string;
    name: string;
    department?: string | null;
    designation?: string | null;
    tasks: Array<{ title: string; description?: string; dueDays?: number }>;
    userId: string;
  }): Promise<HrOnboardingTemplate>;
  listHrOnboardingTasks(businessId: string, employeeId: string): Promise<HrOnboardingTask[]>;
  createHrOnboardingTasksFromTemplate(input: {
    businessId: string;
    employeeId: string;
    templateId: string;
    assignedTo?: string | null;
    userId: string;
  }): Promise<HrOnboardingTask[]>;
  updateHrOnboardingTask(
    id: string,
    patch: {
      status?: "pending" | "in_progress" | "completed" | "blocked";
      dueDate?: string | null;
      assignedTo?: string | null;
      notes?: string | null;
    }
  ): Promise<HrOnboardingTask | undefined>;
  // ── Teams (native People module) ──────────────────────────────────────
  listTeams(businessId: string): Promise<Team[]>;
  getTeamById(id: string): Promise<Team | undefined>;
  createTeam(input: { businessId: string; name: string; description?: string | null; color?: string | null; leadUserId?: string | null; createdBy: string }): Promise<Team>;
  updateTeam(id: string, patch: { name?: string; description?: string | null; color?: string | null; leadUserId?: string | null; isActive?: boolean }): Promise<Team | undefined>;
  deleteTeam(id: string): Promise<boolean>;
  listTeamMembers(teamId: string): Promise<(TeamMember & { user: Pick<User, "id" | "fullName" | "email" | "phone" | "profileImageUrl" | "role"> })[]>;
  addTeamMember(input: { teamId: string; businessId: string; userId: string; role?: string | null }): Promise<TeamMember>;
  removeTeamMember(teamId: string, userId: string): Promise<boolean>;
  getTeamsByUserId(businessId: string, userId: string): Promise<Team[]>;

  getBusinessById(id: string): Promise<Business | undefined>;
  /** Find a business by its assigned account number (e.g. "HEB0001"). Used to match incoming C2B paybill payments. */
  getBusinessByAccountNumber(accountNumber: string): Promise<Business | undefined>;
  getBusinessesByName(name: string): Promise<Business[]>;
  updateBusinessKyc(id: string, data: Partial<InsertBusiness>): Promise<Business | undefined>;
  updateBusinessLogoUrl(id: string, companyLogoUrl: string): Promise<Business | undefined>;
  updateBusinessUserId(id: string, userId: string): Promise<Business | undefined>;
  /** CentyPack org tree + kill switch — super_admin only at HTTP layer. */
  updateBusinessCentyPackOrg(
    id: string,
    patch: { parentBusinessId?: string | null; centypackDisabled?: boolean; centypackBetaEnabled?: boolean },
  ): Promise<Business | undefined>;
  /** Company admin: which Packhouse hub tiles to hide for this business. */
  updateBusinessCentyPackHubHiddenTiles(id: string, hiddenTileIds: string[]): Promise<Business | undefined>;
  createCentyPackMirrorSyncLog(input: {
    businessId: string;
    businessName?: string | null;
    source: string;
    endpoint: string;
    attempted: boolean;
    ok: boolean;
    statusCode?: number | null;
    error?: string | null;
    payload?: Record<string, unknown> | null;
    retryCount?: number;
    nextRetryAt?: Date | null;
  }): Promise<CentypackMirrorSyncLog>;
  listCentyPackMirrorSyncLogs(businessId: string, limit?: number): Promise<CentypackMirrorSyncLog[]>;
  listPendingCentyPackMirrorRetries(limit?: number): Promise<CentypackMirrorSyncLog[]>;
  markCentyPackMirrorSyncLogResult(
    id: string,
    patch: { ok: boolean; statusCode?: number | null; error?: string | null; retryCount?: number; nextRetryAt?: Date | null },
  ): Promise<CentypackMirrorSyncLog | undefined>;
  listLaborWorkers(businessId: string): Promise<LaborWorker[]>;
  createLaborWorker(input: InsertLaborWorker): Promise<LaborWorker>;
  listLaborPieceRates(businessId: string): Promise<LaborPieceRate[]>;
  createLaborPieceRate(input: InsertLaborPieceRate): Promise<LaborPieceRate>;
  listLaborDailyEntries(businessId: string, fromDate: string, toDate: string): Promise<Array<LaborDailyEntry & { workerName: string }>>;
  createLaborDailyEntry(input: InsertLaborDailyEntry): Promise<LaborDailyEntry>;
  createLaborPayrollRun(input: InsertLaborPayrollRun): Promise<LaborPayrollRun>;
  createLaborPayrollLines(lines: InsertLaborPayrollLine[]): Promise<LaborPayrollLine[]>;
  listLaborPayrollRuns(businessId: string, limit?: number): Promise<LaborPayrollRun[]>;
  /** Generate next account number (2 letters + B + 4-digit sequence, e.g. HEB0001) and assign to business. Returns updated business or undefined. */
  assignCompanyAccountNumber(businessId: string, businessName: string): Promise<Business | undefined>;
  /** Generate next account number (2 letters + C + 4-digit sequence, e.g. DCC0002) and assign to user (customer/employee). Returns updated user or undefined. */
  assignCustomerAccountNumber(userId: string, fullName: string): Promise<User | undefined>;
  /** Businesses with no account number (for backfill). */
  getBusinessesWithoutAccountNumber(): Promise<Business[]>;
  /** Users with no account number (for backfill). */
  getUsersWithoutAccountNumber(): Promise<User[]>;

  // Approvals
  createApproval(approval: InsertApproval): Promise<Approval>;
  getApprovals(status?: string, makerIds?: string[]): Promise<Approval[]>;
  getApprovalById(id: string): Promise<Approval | undefined>;
  getLatestApprovalForEntity(entityType: string, entityId: string): Promise<Approval | undefined>;
  checkerAction(id: string, checkerId: string, checkerName: string, action: "approve" | "reject", note?: string): Promise<Approval | undefined>;
  generateOtp(id: string, method: string): Promise<{ otp: string; approval: Approval } | undefined>;
  verifyOtpAndApprove(id: string, code: string, approverId: string, approverName: string, note?: string): Promise<Approval | undefined>;
  rejectApproval(id: string, rejectorId: string, rejectorName: string, note?: string): Promise<Approval | undefined>;

  // Bulk Payments
  createBatch(batch: InsertBulkPaymentBatch): Promise<BulkPaymentBatch>;
  getBatches(userId?: string): Promise<BulkPaymentBatch[]>;
  getBatchesByUserIds(
    userIds: string[],
    options?: { walletBusinessId?: string | null },
  ): Promise<BulkPaymentBatch[]>;
  /**
   * Earliest time any bulk line reached `completed` for batches created by one of the given users.
   * Excludes batches whose creator belongs to a sandbox business. Used for lending qualification (≈90 days).
   */
  getFirstSuccessfulBulkPayoutAt(actorUserIds: string[]): Promise<Date | null>;
  getBatch(id: string): Promise<BulkPaymentBatch | undefined>;
  updateBatchStatus(id: string, status: string): Promise<BulkPaymentBatch | undefined>;
  createPaymentItem(item: InsertBulkPaymentItem): Promise<BulkPaymentItem>;
  getPaymentItemsByBatch(batchId: string): Promise<BulkPaymentItem[]>;
  getPaymentItemsByBatchIds(batchIds: string[]): Promise<BulkPaymentItem[]>;
  getPaymentItemById(id: string): Promise<BulkPaymentItem | undefined>;
  updatePaymentItemStatus(id: string, status: "pending" | "processing" | "completed" | "failed" | "float_hold", failureReason?: string): Promise<BulkPaymentItem | undefined>;
  /**
   * Atomically move a bulk line from `pending` or `float_hold` to `processing` for B2C dispatch.
   * Returns whether this caller won the transition (avoids duplicate Daraja calls).
   */
  tryClaimBulkItemForB2cDispatch(itemId: string): Promise<{ claimed: boolean; row: BulkPaymentItem | undefined }>;
  /** Atomically pending → processing for B2B paybill/till (avoids duplicate Daraja under concurrency). */
  tryClaimBulkItemForB2bDispatch(itemId: string): Promise<{ claimed: boolean; row: BulkPaymentItem | undefined }>;
  updatePaymentItemFromMpesaCallback(itemId: string, status: "completed" | "failed" | "float_hold", opts?: { failureReason?: string; mpesaTransactionId?: string; recipientNameFromMpesa?: string }): Promise<BulkPaymentItem | undefined>;
  /** B2B idempotent result: set official ReceiverPartyPublicName without re-stamping status/processedAt. */
  patchBulkPaymentItemRecipientNameFromMpesa(itemId: string, name: string): Promise<void>;
  getPaymentItemsInFloatHold(): Promise<BulkPaymentItem[]>;
  getPaymentItemsInFloatHoldOlderThan(hours: number): Promise<BulkPaymentItem[]>;
  getLatestAccountBalance(): Promise<{ totalKes: number; rawValue: string; recordedAt: Date } | null>;
  setItemReversalPending(itemId: string, conversationId: string): Promise<BulkPaymentItem | undefined>;
  updateItemReversalStatus(conversationId: string, status: "success" | "failed", resultDesc?: string): Promise<BulkPaymentItem | undefined>;
  finalizeBatchStatus(batchId: string): Promise<BulkPaymentBatch | undefined>;
  /** Enqueue B2C work (skips rows if item already has pending/leased queue row). */
  enqueueB2cDispatchJobs(input: {
    batchId: string;
    itemIds: string[];
    businessId: string | null;
    userId?: string | null;
    userName?: string | null;
  }): Promise<number>;
  countB2cDispatchActiveForBatch(batchId: string): Promise<number>;
  /** Delete queue row only if still held by this lease owner (anti-tamper). */
  completeB2cDispatchJob(id: string, leaseOwner: string): Promise<boolean>;
  /** Return row to pending only if still held by this lease owner. */
  requeueB2cDispatchJob(id: string, leaseOwner: string): Promise<boolean>;
  /** Cluster-wide fair claim; run inside short transaction on dedicated client. */
  claimFairB2cDispatchJobsTx(
    client: PoolClient,
    limit: number,
    leaseOwner: string,
    leaseSeconds: number,
  ): Promise<B2cDispatchQueueRow[]>;
  releaseExpiredB2cDispatchLeasesTx(client: PoolClient): Promise<void>;
  getExpenseJournalPage(actorIds: string[], opts: {
    page: number; limit: number;
    status?: string; category?: string; search?: string;
    dateFrom?: Date; dateTo?: Date;
  }): Promise<{ items: ExpenseJournalRow[]; total: number; totalAmount: string; byCategory: [string, number][] }>;
  updatePaymentItemCategory(itemId: string, category: string): Promise<void>;
  updatePaymentItemStatus(id: string, status: "pending" | "processing" | "completed" | "failed" | "float_hold"): Promise<BulkPaymentItem | undefined>;
  getDueScheduledB2CBatches(limit?: number): Promise<BulkPaymentBatch[]>;
  getScheduledBatches(userId: string, limit?: number): Promise<BulkPaymentBatch[]>;
  getScheduledBatchesByBusiness(businessId: string, opts: { page: number; limit: number; status?: string }): Promise<{ batches: BulkPaymentBatch[]; total: number }>;
  getAllScheduledBatchesPaginated(opts: { page: number; limit: number; status?: string; actorIds?: string[] }): Promise<{ batches: BulkPaymentBatch[]; total: number }>;
  holdBatch(batchId: string, heldBy: string, heldByName: string, reason: string): Promise<BulkPaymentBatch | undefined>;
  releaseBatch(batchId: string): Promise<BulkPaymentBatch | undefined>;
  getHeldBatchesByBusiness(businessId: string, opts: { page: number; limit: number }): Promise<{ batches: BulkPaymentBatch[]; total: number }>;
  // Stale payment reconciliation
  getStaleProcessingItems(olderThanMinutes?: number, limit?: number): Promise<BulkPaymentItem[]>;
  getStalePendingItems(olderThanMinutes?: number, limit?: number): Promise<BulkPaymentItem[]>;
  incrementItemQueryAttempt(itemId: string): Promise<void>;
  forceResolveItem(itemId: string, status: "completed" | "failed", reason?: string): Promise<BulkPaymentItem | undefined>;
  getStaleItemsForReconciliation(opts: { page: number; limit: number; businessId?: string; status?: string; search?: string }): Promise<{ items: any[]; total: number }>;
  searchPaymentItemsForResolution(q: string): Promise<{ items: any[] }>;
  /** Latest M-Pesa recipient name we have for this phone (from past completed B2C callbacks), scoped to company. */
  getMpesaRecipientNameByPhone(businessId: string, normalizedPhone254: string): Promise<string | null>;
  /** Till display: M-Pesa B2B name when present, else user store label from item (same row). */
  getTillRecipientNameByTill(businessId: string, tillNumber: string): Promise<string | null>;
  /** Quick Pay favorite name for this till (buy_goods), same user — helps before/history without M-Pesa name. */
  getQuickPayBuyGoodsTillLabel(userId: string, tillNumber: string): Promise<string | null>;
  /** True if this company has any completed buy-goods item to this till with an M-Pesa transaction id. */
  hasCompletedBuyGoodsPayoutToTill(businessId: string, tillNumber: string): Promise<boolean>;
  /** Latest recipient name we have for this paybill (and optionally account) from past B2B callbacks, scoped to company. */
  getPaybillRecipientName(businessId: string, paybillNumber: string, accountNumber?: string): Promise<string | null>;
  getUserWalletBalance(
    userId: string,
    opts?: { workspaceBusinessId?: string | null },
  ): Promise<{
    currentBalance: string;
    reservedDisbursements: string;
    availableBalance: string;
    overdraftLimit?: string | null;
    overdraftUsed?: string;
    overdraftRemaining?: string;
    effectiveAvailable?: string;
    overdraftDrawnAt?: Date | null;
    hasApprovedCashAdvance?: boolean;
  }>;
  ensureEmployeeWalletAccount(input: {
    businessId: string;
    employeeName: string;
    employeeId?: string | null;
    userId?: string | null;
  }): Promise<EmployeeWalletAccount>;
  getEmployeeWalletAccountForUser(userId: string): Promise<EmployeeWalletAccount | undefined>;
  getEmployeeWalletStatementForUser(
    userId: string,
    opts: { page: number; limit: number }
  ): Promise<{ wallet: EmployeeWalletAccount; entries: EmployeeWalletLedgerEntry[]; total: number; balance: string } | null>;
  getEmployeeWalletBalanceForUser(userId: string): Promise<{ wallet: EmployeeWalletAccount; balance: string } | null>;
  debitEmployeeWallet(input: {
    userId: string;
    businessId: string;
    amount: string;
    entryType: string;
    sourceRef?: string | null;
    description?: string | null;
    meta?: Record<string, unknown>;
  }): Promise<{ wallet: EmployeeWalletAccount; entry: EmployeeWalletLedgerEntry; balance: string }>;
  releaseEmployeeWalletReserveForItem(input: {
    itemId: string;
    reason: string;
    releaseType: "payout_refund" | "payout_scheduled_release";
  }): Promise<boolean>;
  /**
   * After a false "failed" (e.g. stale poller) when M-Pesa actually paid: reverse the
   * erroneous payout_refund / payout_scheduled_release credit by posting a matching debit.
   */
  reapplyWalletDebitAfterErroneousPayoutRelease(itemId: string, reason: string): Promise<boolean>;
  getEmployeeWalletReserveExceptions(input: {
    businessId: string;
    limit?: number;
    graceMinutes?: number;
  }): Promise<{
    scanned: number;
    exceptions: Array<{
      itemId: string | null;
      batchId: string | null;
      walletId: string;
      debitEntryId: string;
      amount: string;
      entryType: string;
      itemStatus: string | null;
      scheduledFor: string | null;
      releasePresent: boolean;
      category:
        | "orphan_debit_no_item"
        | "failed_without_release"
        | "stuck_without_resolution"
        | "completed_but_released_conflict";
      debitCreatedAt: string;
      ageMinutes: number;
    }>;
  }>;
  creditEmployeeWalletFromExpense(input: {
    businessId: string;
    employeeName: string;
    employeeId?: string | null;
    userId?: string | null;
    amount: string;
    expenseClaimId: string;
    note?: string | null;
    actorUserId: string;
  }): Promise<{ wallet: EmployeeWalletAccount; entry: EmployeeWalletLedgerEntry; balance: string }>;
  getEmployeeWalletFundingAnalytics(businessId: string, opts?: { dateFrom?: Date; dateTo?: Date }): Promise<{
    totalFunded: string;
    fundedCount: number;
    uniqueWallets: number;
  }>;

  // Cash Advances
  createAdvance(advance: InsertCashAdvance): Promise<CashAdvance>;
  getAdvances(): Promise<CashAdvance[]>;
  getAdvancesByBusinessId(businessId: string): Promise<CashAdvance[]>;
  getAdvancesExcludingSandbox(): Promise<CashAdvance[]>;
  /** Admin list: advances (excluding sandbox) with embedded business, paginated. */
  getAdminAdvancesEnriched(opts: { limit: number; offset: number }): Promise<{ data: Array<CashAdvance & { business?: Business | null }>; total: number }>;
  /** Admin overview totals for advances (excluding sandbox). */
  getAdminAdvancesOverview(): Promise<{
    totalRequested: string;
    totalActive: string;
    totalRepaid: string;
    pendingCount: number;
    countsByStatus: Array<{ status: string; count: number }>;
    monthly: Array<{ month: string; amount: string; count: number }>;
  }>;
  getAdvance(id: string): Promise<CashAdvance | undefined>;
  updateAdvanceStatus(id: string, status: string): Promise<CashAdvance | undefined>;
  reviewAdvance(
    id: string,
    status: string,
    reviewedBy: string,
    reviewerName: string,
    note?: string,
    approvedAmount?: string | null,
  ): Promise<CashAdvance | undefined>;

  // Overdraft Limits
  createOverdraftLimit(limit: InsertOverdraftLimit): Promise<OverdraftLimit>;
  getOverdraftLimits(): Promise<OverdraftLimit[]>;
  getOverdraftLimitsPaginated(limit: number, offset: number): Promise<{ data: OverdraftLimit[]; total: number }>;
  getOverdraftLimitsOverview(): Promise<{ totalRequested: string; totalApproved: string; totalCurrentBalance: string; pendingCount: number; approvedCount: number; utilization: string; countsByStatus: Array<{ status: string; count: number }> }>;
  getOverdraftLimitById(id: string): Promise<OverdraftLimit | undefined>;
  getOverdraftLimitByBusinessId(businessId: string): Promise<OverdraftLimit | undefined>;
  getOverdraftLimitsByBusinessId(businessId: string): Promise<OverdraftLimit[]>;
  reviewOverdraftLimit(id: string, status: string, approvedLimit: string | null, reviewedBy: string, reviewerName: string, note?: string): Promise<OverdraftLimit | undefined>;

  // Investor Profiles
  createInvestorProfile(profile: InsertInvestorProfile): Promise<InvestorProfile>;
  getInvestorProfiles(): Promise<InvestorProfile[]>;
  getInvestorProfileById(id: string): Promise<InvestorProfile | undefined>;
  updateInvestorProfile(id: string, data: Partial<InsertInvestorProfile>): Promise<InvestorProfile | undefined>;
  updateInvestorKycStatus(id: string, status: string): Promise<InvestorProfile | undefined>;

  // Investments
  createInvestment(investment: InsertInvestment): Promise<Investment>;
  getInvestments(): Promise<Investment[]>;
  getInvestment(id: string): Promise<Investment | undefined>;

  // Transactions
  createTransaction(transaction: InsertTransaction): Promise<Transaction>;
  /** Dedupe platform ledger rows when Safaricom retries the same result callback. */
  findTransactionByReference(reference: string): Promise<Transaction | undefined>;
  getTransactions(limit?: number): Promise<Transaction[]>;

  // Saved Templates
  createSavedTemplate(template: InsertSavedTemplate): Promise<SavedTemplate>;
  getSavedTemplates(userId?: string, businessId?: string): Promise<SavedTemplate[]>;
  updateSavedTemplate(id: string, userId: string, businessId: string | undefined, data: Partial<Pick<SavedTemplate, "name" | "description" | "type" | "data" | "recipientCount">>): Promise<SavedTemplate | undefined>;
  deleteSavedTemplate(id: string, userId?: string, businessId?: string): Promise<void>;

  // Recurring Payments
  createRecurringPayment(payment: InsertRecurringPayment): Promise<RecurringPayment>;
  getRecurringPayments(): Promise<RecurringPayment[]>;
  getRecurringPaymentById(id: string): Promise<RecurringPayment | undefined>;
  updateRecurringPaymentStatus(id: string, status: string): Promise<RecurringPayment | undefined>;
  updateRecurringPaymentApproval(id: string, approvalStatus: string, approvedBy?: string, approvedByName?: string, approvedAt?: Date, approvalId?: string): Promise<RecurringPayment | undefined>;
  updateRecurringPayment(id: string, data: Partial<{ name: string; description: string | null; frequency: string; dayOfWeek: number | null; dayOfMonth: number | null; nextPaymentDate: Date; endDate: Date | null; maxExecutions: number | null; amount: string; recipient: string; accountNumber: string | null; }>): Promise<RecurringPayment | undefined>;
  deleteRecurringPayment(id: string): Promise<void>;
  getRecurringPaymentExecutionHistory(rpId: string, limit?: number): Promise<Array<{ batchId: string; status: string; totalAmount: string; totalFees: string; recipientCount: number; completedCount: number; failedCount: number; createdAt: Date; items: Array<{ id: string; recipient: string; amount: string; fee: string; status: string; failureReason: string | null; }>; }>>;
  getUsersByRoleInBusiness(businessId: string, roles: string[]): Promise<{ id: string; email: string; name: string }[]>;
  getUserRoles(userId: string): Promise<string[]>;
  addUserRole(userId: string, roleSlug: string, assignedBy?: string): Promise<void>;
  removeUserRole(userId: string, roleSlug: string): Promise<void>;
  setUserRoles(userId: string, slugs: string[], assignedBy?: string): Promise<void>;

  // Leave management
  getLeavePolicyByBusinessId(businessId: string): Promise<LeavePolicy | null>;
  upsertLeavePolicy(businessId: string, data: Partial<InsertLeavePolicy>): Promise<LeavePolicy>;
  getLeaveBalances(employeeErpId: string, leaveYear: number): Promise<LeaveBalance[]>;
  getLeaveBalancesByYearStart(employeeErpId: string, yearStartDate: string): Promise<LeaveBalance[]>;
  getLeaveBalanceByUserId(userId: string, leaveType: string, leaveYear: number): Promise<LeaveBalance | null>;
  getLeaveBalance(employeeErpId: string, leaveType: string, leaveYear: number): Promise<LeaveBalance | null>;
  getLeaveBalanceByYearStart(employeeErpId: string, leaveType: string, yearStartDate: string): Promise<LeaveBalance | null>;
  upsertLeaveBalance(data: Omit<InsertLeaveBalance, "id" | "createdAt" | "updatedAt">): Promise<LeaveBalance>;
  addLeaveBalanceUsed(employeeErpId: string, leaveType: string, leaveYear: number, days: number, forDate?: string): Promise<void>;
  subtractLeaveBalanceUsed(employeeErpId: string, leaveType: string, leaveYear: number, days: number, forDate?: string): Promise<void>;
  getAllAnnualLeaveBalancesWithYearStart(): Promise<LeaveBalance[]>;
  getAllAnnualLeaveBalancesWithCarryForward(): Promise<LeaveBalance[]>;
  updateLeaveBalanceCarryForward(id: string, carriedForwardDays: number, carriedForwardAt: Date | null): Promise<void>;
  // Leave forfeiture/warning logs
  createLeaveForfeitureLog(data: Omit<InsertLeaveForfeitureLog, "id" | "forfeitedAt">): Promise<LeaveForfeitureLog>;
  getLeaveForfeitureLogs(businessId: string, limit?: number): Promise<LeaveForfeitureLog[]>;
  createLeaveWarningLog(data: Omit<InsertLeaveWarningLog, "id" | "sentAt">): Promise<LeaveWarningLog>;
  hasLeaveWarningBeenSent(employeeErpId: string, warningType: string, leaveYear: number): Promise<boolean>;

  // Leave applications
  createLeaveApplication(data: Omit<InsertLeaveApplication, "id" | "createdAt" | "updatedAt">): Promise<LeaveApplication>;
  createSalaryAdvanceApproval(row: {
    businessId: string;
    erpAdvanceName: string;
    employeeErpId: string | null;
    approverUserId: string;
    action: "approved" | "rejected";
    remarks: string | null;
  }): Promise<void>;
  getSalaryAdvanceApprovalByErpName(
    businessId: string,
    erpAdvanceName: string,
  ): Promise<{
    action: string;
    remarks: string | null;
    createdAt: Date;
    approverName: string | null;
  } | null>;
  getLeaveApplication(id: string): Promise<LeaveApplication | null>;
  listLeaveApplications(businessId: string, opts: {
    page: number; pageSize: number; status?: string; employeeErpId?: string; userId?: string;
  }): Promise<{ rows: LeaveApplication[]; hasMore: boolean }>;
  updateLeaveApplicationStatus(id: string, status: string, extra?: {
    reviewedByUserId?: string; rejectionReason?: string; leaveApproverEmail?: string;
  }): Promise<LeaveApplication | null>;
  countPendingLeaveApplications(businessId: string, scope: {
    userId?: string; employeeErpId?: string;
  }): Promise<number>;
  hasFuturePendingLeaveApplication(businessId: string, scope: { employeeErpId?: string; userId?: string }): Promise<boolean>;
  getApprovedLeaveDays(businessId: string, scope: { employeeErpId?: string; userId?: string }, leaveType: string, yearStart: string, yearEnd: string): Promise<number>;
  hasApprovedFullDayLeaveOnDate(employeeErpId: string, date: string): Promise<boolean>;

  // Compulsory leave
  createCompulsoryLeave(data: Omit<InsertCompulsoryLeave, "id" | "createdAt">): Promise<CompulsoryLeave>;
  getCompulsoryLeavesByBusiness(businessId: string): Promise<CompulsoryLeave[]>;
  getActiveCompulsoryLeaves(employeeErpId: string, date: string): Promise<CompulsoryLeave[]>;
  getActiveCompulsoryLeavesByUserId(userId: string, date: string): Promise<CompulsoryLeave[]>;
  getCompulsoryLeavesInRange(employeeErpId: string, from: string, to: string): Promise<CompulsoryLeave[]>;
  revokeCompulsoryLeave(id: string, revokedByUserId: string): Promise<CompulsoryLeave | null>;

  // Overtime policy
  getOvertimePolicyByBusinessId(businessId: string): Promise<OvertimePolicy | null>;
  upsertOvertimePolicy(businessId: string, data: Partial<InsertOvertimePolicy>): Promise<OvertimePolicy>;

  // CentyPack farmers
  listCentypackFarmers(businessId: string, opts?: { search?: string; status?: string; limit?: number; offset?: number }): Promise<{ rows: CentypackFarmer[]; total: number }>;
  getCentypackFarmerById(id: string, businessId: string): Promise<CentypackFarmer | null>;
  createCentypackFarmer(data: Omit<InsertCentypackFarmer, "id" | "createdAt" | "updatedAt">): Promise<CentypackFarmer>;
  updateCentypackFarmer(id: string, businessId: string, data: Partial<Omit<InsertCentypackFarmer, "id" | "businessId" | "farmerCode" | "createdAt" | "updatedAt">>): Promise<CentypackFarmer | null>;
  nextCentypackFarmerCode(businessId: string): Promise<string>;

  // CentyPack varieties
  listCentypackVarieties(businessId: string, opts?: { search?: string; cropId?: string; status?: string; limit?: number; offset?: number }): Promise<{ rows: CentypackVariety[]; total: number }>;
  getCentypackVarietyById(id: string, businessId: string): Promise<CentypackVariety | null>;
  createCentypackVariety(data: Omit<InsertCentypackVariety, "id" | "createdAt" | "updatedAt">): Promise<CentypackVariety>;
  updateCentypackVariety(id: string, businessId: string, data: Partial<Omit<InsertCentypackVariety, "id" | "businessId" | "varietyCode" | "createdAt" | "updatedAt">>): Promise<CentypackVariety | null>;
  nextCentypackVarietyCode(businessId: string): Promise<string>;

  // CentyPack carton types
  listCentypackCartonTypes(businessId: string, opts?: { search?: string; status?: string; limit?: number; offset?: number }): Promise<{ rows: CentypackCartonType[]; total: number }>;
  getCentypackCartonTypeById(id: string, businessId: string): Promise<CentypackCartonType | null>;
  createCentypackCartonType(data: Omit<InsertCentypackCartonType, "id" | "createdAt" | "updatedAt">): Promise<CentypackCartonType>;
  updateCentypackCartonType(id: string, businessId: string, data: Partial<Omit<InsertCentypackCartonType, "id" | "businessId" | "cartonCode" | "createdAt" | "updatedAt">>): Promise<CentypackCartonType | null>;
  nextCentypackCartonTypeCode(businessId: string): Promise<string>;

  // CentyPack worker categories
  listCentypackWorkerCategories(businessId: string, opts?: { search?: string; status?: string; limit?: number; offset?: number }): Promise<{ rows: CentypackWorkerCategory[]; total: number }>;
  getCentypackWorkerCategoryById(id: string, businessId: string): Promise<CentypackWorkerCategory | null>;
  createCentypackWorkerCategory(data: Omit<InsertCentypackWorkerCategory, "id" | "createdAt" | "updatedAt">): Promise<CentypackWorkerCategory>;
  updateCentypackWorkerCategory(id: string, businessId: string, data: Partial<Omit<InsertCentypackWorkerCategory, "id" | "businessId" | "workerCategoryCode" | "createdAt" | "updatedAt">>): Promise<CentypackWorkerCategory | null>;
  nextCentypackWorkerCategoryCode(businessId: string): Promise<string>;

  // CentyPack warehouses
  listCentypackWarehouses(businessId: string, opts?: { search?: string; status?: string; limit?: number; offset?: number }): Promise<{ rows: CentypackWarehouse[]; total: number }>;
  getCentypackWarehouseById(id: string, businessId: string): Promise<CentypackWarehouse | null>;
  createCentypackWarehouse(data: Omit<InsertCentypackWarehouse, "id" | "createdAt" | "updatedAt">): Promise<CentypackWarehouse>;
  updateCentypackWarehouse(id: string, businessId: string, data: Partial<Omit<InsertCentypackWarehouse, "id" | "businessId" | "warehouseCode" | "createdAt" | "updatedAt">>): Promise<CentypackWarehouse | null>;
  nextCentypackWarehouseCode(businessId: string): Promise<string>;

  // CentyPack grade codes
  listCentypackGradeCodes(businessId: string, opts?: { search?: string; status?: string; limit?: number; offset?: number }): Promise<{ rows: CentypackGradeCode[]; total: number }>;
  getCentypackGradeCodeById(id: string, businessId: string): Promise<CentypackGradeCode | null>;
  createCentypackGradeCode(data: Omit<InsertCentypackGradeCode, "id" | "createdAt" | "updatedAt">): Promise<CentypackGradeCode>;
  updateCentypackGradeCode(id: string, businessId: string, data: Partial<Omit<InsertCentypackGradeCode, "id" | "businessId" | "gradeCode" | "createdAt" | "updatedAt">>): Promise<CentypackGradeCode | null>;
  nextCentypackGradeCode(businessId: string): Promise<string>;

  // CentyPack defect types
  listCentypackDefectTypes(businessId: string, opts?: { search?: string; status?: string; limit?: number; offset?: number }): Promise<{ rows: CentypackDefectType[]; total: number }>;
  getCentypackDefectTypeById(id: string, businessId: string): Promise<CentypackDefectType | null>;
  createCentypackDefectType(data: Omit<InsertCentypackDefectType, "id" | "createdAt" | "updatedAt">): Promise<CentypackDefectType>;
  updateCentypackDefectType(id: string, businessId: string, data: Partial<Omit<InsertCentypackDefectType, "id" | "businessId" | "defectCode" | "createdAt" | "updatedAt">>): Promise<CentypackDefectType | null>;
  nextCentypackDefectTypeCode(businessId: string): Promise<string>;

  // CentyPack intake
  listCentypackIntake(businessId: string, opts?: { search?: string; farmerId?: string; cropId?: string; status?: string; dateFrom?: string; dateTo?: string; limit?: number; offset?: number; excludeGraded?: boolean; includeIntakeId?: string }): Promise<{ rows: CentypackIntake[]; total: number }>;
  getCentypackIntakeById(id: string, businessId: string): Promise<CentypackIntake | null>;
  createCentypackIntake(data: Omit<InsertCentypackIntake, "id" | "createdAt" | "updatedAt">): Promise<CentypackIntake>;
  updateCentypackIntake(id: string, businessId: string, data: Partial<Omit<InsertCentypackIntake, "id" | "businessId" | "intakeCode" | "createdAt" | "updatedAt">>): Promise<CentypackIntake | null>;
  nextCentypackIntakeCode(businessId: string): Promise<string>;
  nextCentypackBatchCode(businessId: string): Promise<string>;

  // CentyPack GDNs
  listCentypackGdns(businessId: string, opts?: { search?: string; type?: string; status?: string; dateFrom?: string; dateTo?: string; limit?: number; offset?: number }): Promise<{ rows: (CentypackGdn & { totalWeightKg: number; itemCount: number; batchCodes: string[] })[]; total: number }>;
  getCentypackGdnById(id: string, businessId: string): Promise<(CentypackGdn & { items: CentypackGdnItem[] }) | null>;
  createCentypackGdn(header: Omit<InsertCentypackGdn, "id" | "createdAt" | "updatedAt">, items: Omit<InsertCentypackGdnItem, "id" | "gdnId" | "businessId" | "createdAt">[]): Promise<CentypackGdn & { items: CentypackGdnItem[] }>;
  updateCentypackGdn(id: string, businessId: string, header: Partial<Omit<InsertCentypackGdn, "id" | "businessId" | "gdnCode" | "createdAt" | "updatedAt">>, items?: Omit<InsertCentypackGdnItem, "id" | "gdnId" | "businessId" | "createdAt">[]): Promise<(CentypackGdn & { items: CentypackGdnItem[] }) | null>;
  nextCentypackGdnCode(businessId: string): Promise<string>;

  // CentyPack grading sessions
  nextCentypackGradingCode(businessId: string): Promise<string>;
  listCentypackGradingSessions(businessId: string, opts?: { search?: string; cropId?: string; status?: string; dateFrom?: string; dateTo?: string; limit?: number; offset?: number }): Promise<{ rows: (CentypackGradingSession & { totalGradedKg: number; totalDefectKg: number })[]; total: number }>;
  getCentypackGradingSessionById(id: string, businessId: string): Promise<(CentypackGradingSession & { lines: CentypackGradingLine[]; defects: CentypackGradingDefect[] }) | null>;
  createCentypackGradingSession(header: Omit<InsertCentypackGradingSession, "id" | "createdAt" | "updatedAt">, lines: Omit<InsertCentypackGradingLine, "id" | "sessionId" | "businessId" | "createdAt">[], defects: Omit<InsertCentypackGradingDefect, "id" | "sessionId" | "businessId" | "createdAt">[]): Promise<CentypackGradingSession & { lines: CentypackGradingLine[]; defects: CentypackGradingDefect[] }>;
  updateCentypackGradingSession(id: string, businessId: string, header: Partial<Omit<InsertCentypackGradingSession, "id" | "businessId" | "sessionCode" | "createdAt" | "updatedAt">>, lines?: Omit<InsertCentypackGradingLine, "id" | "sessionId" | "businessId" | "createdAt">[], defects?: Omit<InsertCentypackGradingDefect, "id" | "sessionId" | "businessId" | "createdAt">[]): Promise<(CentypackGradingSession & { lines: CentypackGradingLine[]; defects: CentypackGradingDefect[] }) | null>;

  // CentyPack pack sessions
  nextCentypackPackCode(businessId: string): Promise<string>;
  listCentypackPackSessions(businessId: string, opts?: { search?: string; cropId?: string; status?: string; dateFrom?: string; dateTo?: string; limit?: number; offset?: number }): Promise<{ rows: (CentypackPackSession & { totalCartons: number; totalWeightKg: number })[]; total: number }>;
  getCentypackPackSessionById(id: string, businessId: string): Promise<(CentypackPackSession & { lines: (CentypackPackLine & { labourers: (CentypackPackLineLabourer & { labourerCode: string; firstName: string; lastName: string })[] })[] }) | null>;
  createCentypackPackSession(header: Omit<InsertCentypackPackSession, "id" | "createdAt" | "updatedAt">, lines: (Omit<InsertCentypackPackLine, "id" | "sessionId" | "businessId" | "createdAt"> & { labourers?: { labourerId: string; cartonCount: number }[] })[]): Promise<CentypackPackSession & { lines: CentypackPackLine[] }>;
  updateCentypackPackSession(id: string, businessId: string, header: Partial<Omit<InsertCentypackPackSession, "id" | "businessId" | "sessionCode" | "createdAt" | "updatedAt">>, lines?: (Omit<InsertCentypackPackLine, "id" | "sessionId" | "businessId" | "createdAt"> & { labourers?: { labourerId: string; cartonCount: number }[] })[], lineLabourers?: Record<string, { labourerId: string; cartonCount: number }[]>): Promise<(CentypackPackSession & { lines: CentypackPackLine[] }) | null>;

  // CentyPack labour
  nextCentypackLabourerCode(businessId: string): Promise<string>;
  listCentypackLabourers(businessId: string, opts?: { search?: string; status?: string; workerCategoryId?: string; limit?: number; offset?: number }): Promise<{ rows: (CentypackLabourer & { workerCategoryName: string | null })[]; total: number }>;
  getCentypackLabourerById(id: string, businessId: string): Promise<CentypackLabourer | null>;
  createCentypackLabourer(data: Omit<InsertCentypackLabourer, "id" | "createdAt" | "updatedAt">): Promise<CentypackLabourer>;
  updateCentypackLabourer(id: string, businessId: string, data: Partial<Omit<InsertCentypackLabourer, "id" | "businessId" | "labourerCode" | "createdAt" | "updatedAt">>): Promise<CentypackLabourer | null>;
  getLabourAttendanceByDate(businessId: string, date: string): Promise<{ labourer: CentypackLabourer & { workerCategoryName: string | null }; attendance: CentypackLabourAttendance | null }[]>;
  upsertLabourAttendance(businessId: string, date: string, records: { labourerId: string; status: string; notes?: string | null }[]): Promise<void>;
  getLabourPackSummary(businessId: string, opts?: { dateFrom?: string; dateTo?: string; labourerId?: string; packSessionId?: string }): Promise<{ labourerId: string; labourerCode: string; firstName: string; lastName: string; packSessionId: string; packSessionCode: string; sessionDate: string; packLineId: string; gradeCodeId: string | null; cartonCount: number }[]>;
  getPackLineLabourers(packLineId: string): Promise<(CentypackPackLineLabourer & { labourerCode: string; firstName: string; lastName: string })[]>;
  upsertPackLineLabourers(packLineId: string, businessId: string, assignments: { labourerId: string; cartonCount: number }[]): Promise<void>;

  // CentyPack stock ledger
  getStockOnHand(businessId: string, opts?: { cropId?: string; stage?: string; warehouseId?: string }): Promise<StockOnHandRow[]>;
  getStockMovements(businessId: string, opts?: { txnId?: string; txnType?: string; cropId?: string; stage?: string; dateFrom?: string; dateTo?: string; limit?: number; offset?: number }): Promise<{ rows: StockMovementRow[]; total: number }>;

  // CentyPack crops
  listCentypackCrops(businessId: string, opts?: { search?: string; status?: string; limit?: number; offset?: number }): Promise<{ rows: CentypackCrop[]; total: number }>;
  getCentypackCropById(id: string, businessId: string): Promise<CentypackCrop | null>;
  createCentypackCrop(data: Omit<InsertCentypackCrop, "id" | "createdAt" | "updatedAt">): Promise<CentypackCrop>;
  updateCentypackCrop(id: string, businessId: string, data: Partial<Omit<InsertCentypackCrop, "id" | "businessId" | "cropCode" | "createdAt" | "updatedAt">>): Promise<CentypackCrop | null>;
  nextCentypackCropCode(businessId: string): Promise<string>;

  // Employee ERP ID / User ID lookup
  upsertEmployeeUserLink(userId: string, erpEmployeeId: string): Promise<void>;
  getEmployeeErpIdByUserId(userId: string): Promise<string | null>;
  getUserIdByEmployeeErpId(employeeErpId: string): Promise<string | null>;

  // Payment Links
  createPaymentLink(
    link: InsertPaymentLink & { createdByUserId?: string | null; createdByName?: string | null },
  ): Promise<PaymentLink>;
  getPaymentLinks(userId?: string): Promise<PaymentLink[]>;
  getPaymentLinksByUserIds(userIds: string[]): Promise<PaymentLink[]>;
  getPaymentLinkById(id: string): Promise<PaymentLink | undefined>;
  getPaymentLinkByToken(token: string): Promise<PaymentLink | undefined>;
  getPaymentLinkByCheckoutRequestId(checkoutRequestId: string): Promise<PaymentLink | undefined>;
  updatePaymentLinkStatus(id: string, status: string): Promise<PaymentLink | undefined>;
  updatePaymentLinkByCheckoutRequestId(checkoutRequestId: string, data: Partial<PaymentLink>): Promise<PaymentLink | undefined>;
  updatePaymentLinkByToken(token: string, data: Partial<PaymentLink>): Promise<PaymentLink | undefined>;
  deletePaymentLink(id: string): Promise<void>;

  // Wallet Top-ups
  createWalletTopup(topup: InsertWalletTopup): Promise<WalletTopup>;
  getWalletTopups(userId?: string): Promise<WalletTopup[]>;
  getWalletTopupsByUserIds(userIds: string[]): Promise<WalletTopup[]>;
  getWalletTopupById(id: string): Promise<WalletTopup | undefined>;
  getWalletTopupByCheckoutRequestId(checkoutRequestId: string): Promise<WalletTopup | undefined>;
  /** Match C2B TransID stored as reference or mpesa_receipt_number (idempotency). */
  getWalletTopupByMpesaReceiptNumber(receipt: string): Promise<WalletTopup | undefined>;
  updateWalletTopupByCheckoutRequestId(checkoutRequestId: string, data: Partial<InsertWalletTopup>): Promise<WalletTopup | undefined>;
  updateWalletTopupStatus(id: string, status: "approved" | "rejected", extra?: { mpesaReceiptNumber?: string; checkoutResultCode?: string; checkoutResultDesc?: string; callbackPayload?: string }): Promise<WalletTopup | undefined>;
  reviewWalletTopup(id: string, reviewedBy: string, reviewerName: string, action: "approved" | "rejected", note?: string): Promise<WalletTopup | undefined>;

  // Payment Settings (per-business)
  getPaymentSettingsByBusinessId(businessId: string): Promise<BusinessPaymentSettings | undefined>;
  upsertPaymentSettings(businessId: string, data: Partial<InsertBusinessPaymentSettings>): Promise<BusinessPaymentSettings>;
  getCompanyCustomRates(businessId: string): Promise<{ useCustomRates: boolean; customFeeSchedule: Array<{ min: number; max: number; fee: number }> | null; customMaxAmount: number | null }>;
  upsertCompanyCustomRates(businessId: string, useCustomRates: boolean, customFeeSchedule: Array<{ min: number; max: number; fee: number }> | null, customMaxAmount: number | null): Promise<void>;
  getPaymentFavoritesByBusinessId(businessId: string): Promise<PaymentFavorite[]>;
  createPaymentFavorite(favorite: InsertPaymentFavorite): Promise<PaymentFavorite>;
  updatePaymentFavorite(id: string, businessId: string, data: Partial<Pick<PaymentFavorite, "name" | "data" | "isActive">>): Promise<PaymentFavorite | undefined>;
  deletePaymentFavorite(id: string, businessId: string): Promise<void>;

  // Dashboard
  getDashboardStats(
    userId: string,
    options?: { rangeDays?: number; workspaceBusinessId?: string | null },
  ): Promise<{
    accountBalance: string;
    totalPayments: string;
    activeCashAdvance: string;
    paymentCount: number;
    pendingLinks: number;
    pendingApprovals: number;
  }>;

  // 2FA
  enableTwoFactor(userId: string, secret: string): Promise<void>;
  disableTwoFactor(userId: string): Promise<void>;
  setUser2faMethod(userId: string, method: "email_otp" | "totp" | null): Promise<void>;
  setCompany2fa(businessId: string, enforced: boolean, method: string | null): Promise<void>;

  // Quick Pay Recipients
  createQuickPayRecipient(recipient: InsertQuickPayRecipient): Promise<QuickPayRecipient>;
  getQuickPayRecipients(userId: string, businessId?: string | null): Promise<QuickPayRecipient[]>;
  getQuickPayRecipientById(id: string): Promise<QuickPayRecipient | undefined>;
  deleteQuickPayRecipient(id: string, userId: string, businessId?: string | null): Promise<void>;

  // M-Pesa Audit Logs
  createMpesaAuditLog(type: string, payload: unknown): Promise<MpesaAuditLog>;
  getMpesaAuditLogs(limit?: number): Promise<MpesaAuditLog[]>;
  getMpesaAuditLogsByTypes(types: string[], limit?: number): Promise<MpesaAuditLog[]>;
  /** Latest audit row of `type` whose JSON/text payload contains `substring` (e.g. item UUID). */
  getLatestMpesaAuditLogMatchingPayload(type: string, payloadSubstring: string): Promise<MpesaAuditLog | undefined>;
  createHakikishaLookup(phone: string, originatorConversationId: string): Promise<HakikishaLookup>;
  updateHakikishaLookupByOriginatorId(
    originatorConversationId: string,
    data: {
      registeredName?: string;
      transactionId?: string;
      status: string;
      mpesaResultCode?: string;
      mpesaResultDesc?: string;
    },
  ): Promise<number>;
  getHakikishaLookupByOriginatorId(originatorConversationId: string): Promise<HakikishaLookup | undefined>;
  /** Latest verified registered name for this phone from a successful Hakikisha lookup (so we skip B2C if we already have it). */
  getHakikishaVerifiedNameByPhone(normalizedPhone254: string): Promise<string | null>;
  getBulkHakikishaCachedNames(businessId: string | null, phones: string[]): Promise<Record<string, string>>;
  /** Pending Hakikisha lookup for this phone (within last 10 min) so we do not send B2C twice before callback. */
  getPendingHakikishaLookupByPhone(normalizedPhone254: string): Promise<{ originatorConversationId: string } | null>;
  syncMpesaTransactions(): Promise<{ synced: number; updated: number; total: number }>;
  getMpesaTransactionById(id: string): Promise<MpesaTransaction | undefined>;
  updateMpesaTransactionStatus(id: string, data: { status: string; mpesaTransactionId?: string; recipientName?: string; processedAt?: Date; failureReason?: string }): Promise<void>;
  findB2CRequestByItemId(itemId: string): Promise<{ conversationId: string; originatorConversationId: string } | null>;
  findB2BRequestByItemId(itemId: string): Promise<{ conversationId: string; originatorConversationId: string } | null>;
  /** Resolve batch/item from b2c_request audit by Safaricom conversation IDs (handles missing Occasion on result). */
  findB2CRequestContextByConversationIds(
    conversationId: string,
    originatorConversationId: string,
  ): Promise<{ batchId?: string; itemId?: string; userId?: string } | null>;
  getMpesaTransactionsPaginated(page: number, pageSize: number, paymentType?: string, status?: string, search?: string): Promise<{
    data: MpesaTransaction[];
    total: number;
    page: number;
    pageSize: number;
    lastSyncedAt: string | null;
  }>;
  getAdminDisbursements(page: number, pageSize: number, paymentType?: string, status?: string): Promise<{
    data: Array<{
      itemId: string;
      batchId: string;
      paymentType: string;
      recipient: string;
      accountNumber: string | null;
      amount: string;
      fee: string;
      reference: string | null;
      status: string;
      failureReason: string | null;
      mpesaTransactionId: string | null;
      recipientName: string | null;
      batchName: string | null;
      createdByName: string | null;
      createdAt: string;
    }>;
    total: number;
    page: number;
    pageSize: number;
  }>;

  // Notifications
  createNotification(notification: InsertNotification): Promise<Notification>;
  getNotifications(userId?: string, limit?: number): Promise<Notification[]>;
  getUnreadNotificationCount(userId?: string): Promise<number>;
  markNotificationRead(id: string, userId: string): Promise<Notification | undefined>;
  markAllNotificationsRead(userId?: string): Promise<void>;

  // Subscriptions
  getSubscriptions(): Promise<Subscription[]>;
  getSubscriptionById(id: string): Promise<Subscription | undefined>;
  getSubscriptionByBusinessId(businessId: string): Promise<Subscription | undefined>;
  createSubscription(sub: InsertSubscription): Promise<Subscription>;
  updateSubscriptionStatus(id: string, status: string): Promise<Subscription | undefined>;
  cancelSubscription(subscriptionId: string): Promise<void>;
  purchaseSubscription(opts: {
    businessId: string;
    businessOwnerUserId: string;
    businessOwnerName: string;
    businessName: string;
    plan: "starter" | "business" | "enterprise";
    billingCycle: "monthly" | "annually";
    amount: number;
  }): Promise<{ subscription: Subscription; invoice: Invoice }>;
  seedDefaultPlanSettings(): Promise<void>;

  // Invoices
  getInvoices(): Promise<Invoice[]>;
  getInvoiceById(id: string): Promise<Invoice | undefined>;
  getInvoicesByBusiness(businessId: string): Promise<Invoice[]>;
  getBusinessSubscriptionInvoices(businessId: string): Promise<Invoice[]>;
  createInvoice(invoice: InsertInvoice): Promise<Invoice>;
  updateInvoiceStatus(id: string, status: string, paidAt?: Date): Promise<Invoice | undefined>;
  // AR core (tenant-scoped)
  createArCustomer(input: InsertArCustomer): Promise<ArCustomer>;
  updateArCustomer(id: string, businessId: string, patch: Partial<InsertArCustomer>): Promise<ArCustomer | undefined>;
  getArCustomerById(id: string, businessId: string): Promise<ArCustomer | undefined>;
  getArCustomerByErpId(erpCustomerId: string, businessId: string): Promise<ArCustomer | undefined>;
  listArCustomersByBusiness(businessId: string): Promise<ArCustomer[]>;
  createArInvoice(input: InsertArInvoice): Promise<ArInvoice>;
  updateArInvoice(id: string, businessId: string, patch: Partial<InsertArInvoice>): Promise<ArInvoice | undefined>;
  getArInvoiceById(id: string, businessId: string): Promise<ArInvoice | undefined>;
  getArInvoiceByErpId(erpInvoiceId: string, businessId: string): Promise<ArInvoice | undefined>;
  listArInvoicesByBusiness(businessId: string): Promise<ArInvoice[]>;
  replaceArInvoiceLines(arInvoiceId: string, lines: Omit<InsertArInvoiceLine, "arInvoiceId">[]): Promise<ArInvoiceLine[]>;
  listArInvoiceLines(arInvoiceId: string): Promise<ArInvoiceLine[]>;
  createArPayment(input: InsertArPayment): Promise<ArPayment>;
  getArPaymentById(id: string, businessId: string): Promise<ArPayment | undefined>;
  getArPaymentByPaymentLinkId(paymentLinkId: string): Promise<ArPayment | undefined>;
  listArPaymentsByBusiness(businessId: string): Promise<ArPayment[]>;
  createArAllocation(input: InsertArAllocation): Promise<ArAllocation>;
  listArAllocationsByInvoice(arInvoiceId: string): Promise<ArAllocation[]>;
  listArAllocationsByPayment(arPaymentId: string): Promise<ArAllocation[]>;
  createArStatementEntry(input: InsertArStatementEntry): Promise<ArStatementEntry>;
  listArStatementEntries(
    businessId: string,
    opts?: { arCustomerId?: string; from?: Date; to?: Date }
  ): Promise<ArStatementEntry[]>;
  getArUnallocatedCreditByCustomer(businessId: string, arCustomerId: string): Promise<string>;

  // Support Tickets
  getTickets(status?: string): Promise<SupportTicket[]>;
  getTicketById(id: string): Promise<SupportTicket | undefined>;
  createTicket(ticket: InsertSupportTicket): Promise<SupportTicket>;
  updateTicketStatus(id: string, status: string, resolution?: string): Promise<SupportTicket | undefined>;
  assignTicket(id: string, assignedTo: string, assignedToName: string): Promise<SupportTicket | undefined>;

  // Ticket Comments
  getTicketComments(ticketId: string): Promise<TicketComment[]>;
  createTicketComment(comment: InsertTicketComment): Promise<TicketComment>;

  // Imported transactions (uploaded from external platforms by super-admin)
  createImportedTransactions(rows: InsertImportedTransaction[]): Promise<ImportedTransaction[]>;
  getImportedTransactionsByBusiness(businessId: string): Promise<ImportedTransaction[]>;
  getImportBatchesByBusiness(businessId: string): Promise<Array<{ importBatchId: string; count: number; sourceFileName: string | null; importedByName: string | null; createdAt: Date }>>;
  deleteImportBatch(importBatchId: string, businessId: string): Promise<void>;

  // Demo requests (marketing → CRM)
  createDemoRequest(row: InsertDemoRequest): Promise<DemoRequest>;
  getDemoRequestsPaginated(limit: number, offset: number): Promise<{ data: DemoRequest[]; total: number }>;
  updateDemoRequestStatus(id: string, status: string): Promise<DemoRequest | undefined>;

  // Admin analytics
  getPlatformWalletSummary(page: number, limit: number, type?: string): Promise<any>;
  getCompaniesOverview(): Promise<any>;
  getWalletsOverview(): Promise<any>;
  getSubscriptionsOverview(): Promise<any>;
  getTicketsOverview(): Promise<any>;
  getAuditTrails(limit?: number): Promise<any>;
  getDailyReconciliation(date?: string): Promise<any>;
  getReportsData(type: string, startDate?: string, endDate?: string): Promise<any>;

  // Paginated queries
  getTransactionsPaginated(page: number, pageSize: number): Promise<PaginatedResult<Transaction>>;
  getApprovalsPaginated(page: number, pageSize: number, status?: string, makerIds?: string[]): Promise<PaginatedResult<Approval>>;
  getBatchesPaginated(page: number, pageSize: number, userId?: string): Promise<PaginatedResult<BulkPaymentBatch>>;
  getAllBatchesAdminPaginated(page: number, pageSize: number, status?: string): Promise<{ batches: BulkPaymentBatch[]; total: number; totalPages: number }>;
  getInvestmentsPaginated(page: number, pageSize: number): Promise<PaginatedResult<Investment>>;
  getInvestorProfilesPaginated(page: number, pageSize: number): Promise<PaginatedResult<InvestorProfile>>;
  getNotificationsPaginated(page: number, pageSize: number, userId?: string): Promise<PaginatedResult<Notification>>;
  getRecurringPaymentsPaginated(page: number, pageSize: number): Promise<PaginatedResult<RecurringPayment>>;
  getRecurringPaymentsByBusinessPaginated(businessId: string, page: number, pageSize: number): Promise<PaginatedResult<RecurringPayment>>;
  getRecurringPaymentsByUserPaginated(userId: string, page: number, pageSize: number): Promise<PaginatedResult<RecurringPayment>>;
  getPaymentLinksPaginated(page: number, pageSize: number, userId?: string): Promise<PaginatedResult<PaymentLink>>;
  getAdvancesPaginated(page: number, pageSize: number): Promise<PaginatedResult<CashAdvance>>;

  // Admin Dashboard
  getAllBusinesses(): Promise<(Business & { user?: User })[]>;
  getBusinessesPaginated(limit: number, offset: number): Promise<{ data: (Business & { user?: User })[]; total: number }>;
  getUsersPaginated(limit: number, offset: number): Promise<{ data: (Omit<User, "passwordHash"> & { companyName: string | null; companyId: string | null })[]; total: number }>;
  getAdminTransactionsPaginated(limit: number, offset: number): Promise<{ data: Transaction[]; total: number }>;
  getAdminDashboardStats(): Promise<{
    totalUsers: number;
    activeUsers: number;
    totalBusinesses: number;
    kycPending: number;
    kycApproved: number;
    kycRejected: number;
    totalTransactionVolume: string;
    totalTransactionCount: number;
    totalAdvancesIssued: string;
    activeAdvances: number;
    pendingAdvances: number;
    totalInvestments: string;
    activeInvestments: number;
    totalPaymentBatches: number;
    totalPaymentBatchAmount: string;
    pendingApprovals: number;
    approvedCount: number;
    rejectedCount: number;
    totalPaymentLinks: number;
    activePaymentLinks: number;
    paidPaymentLinks: number;
    recentUsers: User[];
    recentTransactions: EnrichedAdminTransaction[];
  }>;
}

/**
 * User-entered store label from `reference` when M-Pesa omits till display names on B2B.
 * Ignores auto-generated placeholders (TILL-…, QP-…, EW-…) and the raw till digits alone.
 */
function storeNameFromPaymentReference(reference: string | null | undefined, tillDigits: string): string | null {
  let r = (reference ?? "").trim();
  if (!r) return null;
  if (r === tillDigits) return null;
  if (/^TILL-\d+$/i.test(r)) return null;
  if (/^QP-[A-Z0-9]+$/i.test(r)) return null;
  if (/^EW-[A-Z0-9]+$/i.test(r)) return null;
  if (/^QP\s+/i.test(r)) {
    r = r.replace(/^QP\s+/i, "").trim();
    if (!r) return null;
  }
  return r || null;
}

export class DatabaseStorage implements IStorage {
  // ──────────── Auth ────────────
  async createUser(
    email: string,
    password: string,
    fullName: string,
    phone: string | null,
    role: string,
    createdBy?: string,
    accountType?: string,
    businessId?: string,
    status?: string,
    emailVerifiedAt?: Date | null,
  ): Promise<User> {
    const passwordHash = await bcrypt.hash(password, 10);
    const [result] = await db.insert(users).values({
      id: crypto.randomUUID(),
      email,
      passwordHash,
      fullName,
      phone,
      role: role as any,
      accountType: (accountType || "client") as any,
      status: status || "active",
      businessId: businessId || null,
      createdBy,
      emailVerifiedAt: emailVerifiedAt ?? null,
    }).returning();
    return result;
  }

  async createUserWithGoogle(googleId: string, email: string, fullName: string, profileImageUrl?: string | null): Promise<User> {
    const placeholderHash = await bcrypt.hash(crypto.randomBytes(32).toString("hex"), 10);
    const [result] = await db.insert(users).values({
      id: crypto.randomUUID(),
      email,
      passwordHash: placeholderHash,
      googleId,
      fullName,
      phone: null,
      profileImageUrl: profileImageUrl ?? null,
      role: "viewer",
      accountType: "client",
      emailVerifiedAt: new Date(),
    }).returning();
    return result;
  }

  async getUserByEmail(email: string): Promise<User | undefined> {
    const [result] = await db.select().from(users).where(eq(users.email, email));
    return result;
  }

  async getUserByGoogleId(googleId: string): Promise<User | undefined> {
    const [result] = await db.select().from(users).where(eq(users.googleId, googleId));
    return result;
  }

  async getUserById(id: string): Promise<User | undefined> {
    const [result] = await db.select().from(users).where(eq(users.id, id));
    return result;
  }

  async getUsersByIds(ids: string[]): Promise<User[]> {
    if (!ids.length) return [];
    return db.select().from(users).where(inArray(users.id, ids));
  }

  async getUsers(): Promise<User[]> {
    return db.select().from(users).orderBy(desc(users.createdAt));
  }

  async getUsersByBusinessId(businessId: string): Promise<User[]> {
    const primary = await db
      .select()
      .from(users)
      .where(eq(users.businessId, businessId))
      .orderBy(desc(users.createdAt));

    let linked: { user: User }[] = [];
    try {
      linked = await db
        .select({ user: users })
        .from(userBusinessMemberships)
        .innerJoin(users, eq(userBusinessMemberships.userId, users.id))
        .where(eq(userBusinessMemberships.businessId, businessId));
    } catch (e) {
      if (isMissingRelation(e, "user_business_memberships")) {
        console.warn("[storage] user_business_memberships table missing; using primary business users only");
      } else {
        throw e;
      }
    }

    const dedup = new Map<string, User>();
    for (const u of primary) dedup.set(u.id, u);
    for (const row of linked) dedup.set(row.user.id, row.user);
    return Array.from(dedup.values()).sort((a, b) => {
      const ta = a.createdAt ? new Date(a.createdAt).getTime() : 0;
      const tb = b.createdAt ? new Date(b.createdAt).getTime() : 0;
      return tb - ta;
    });
  }

  async listUserBusinessMemberships(userId: string): Promise<Business[]> {
    const [primaryUser] = await db
      .select({ businessId: users.businessId })
      .from(users)
      .where(eq(users.id, userId))
      .limit(1);

    const out = new Map<string, Business>();
    if (primaryUser?.businessId) {
      const [primaryBiz] = await db.select().from(businesses).where(eq(businesses.id, primaryUser.businessId)).limit(1);
      if (primaryBiz) out.set(primaryBiz.id, primaryBiz);
    }

    try {
      const linked = await db
        .select({ business: businesses })
        .from(userBusinessMemberships)
        .innerJoin(businesses, eq(userBusinessMemberships.businessId, businesses.id))
        .where(
          and(eq(userBusinessMemberships.userId, userId), eq(userBusinessMemberships.membershipStatus, "active")),
        );
      for (const row of linked) out.set(row.business.id, row.business);
    } catch (e) {
      if (isMissingRelation(e, "user_business_memberships")) {
        console.warn("[storage] user_business_memberships table missing; membership list fallback active");
      } else {
        throw e;
      }
    }

    const owned = await db.select().from(businesses).where(eq(businesses.userId, userId));
    for (const b of owned) out.set(b.id, b);

    return Array.from(out.values()).sort((a, b) => a.businessName.localeCompare(b.businessName));
  }

  async listAccessibleBusinessesWithRoles(userId: string): Promise<Array<Business & { accessRole: string }>> {
    const u = await this.getUserById(userId);
    if (!u) return [];
    const list = await this.listUserBusinessMemberships(userId);
    const result: Array<Business & { accessRole: string }> = [];
    for (const b of list) {
      let accessRole: string | null = await this.getUserBusinessMembershipRole(userId, b.id);
      if (!accessRole) {
        if (u.businessId === b.id) {
          accessRole = u.role;
        } else {
          const [row] = await db
            .select({ ownerId: businesses.userId })
            .from(businesses)
            .where(eq(businesses.id, b.id))
            .limit(1);
          if (row?.ownerId === userId) accessRole = u.role;
        }
      }
      if (!accessRole) accessRole = "viewer";
      result.push({ ...b, accessRole });
    }
    return result.sort((a, b) => a.businessName.localeCompare(b.businessName));
  }

  async addUserToBusiness(
    userId: string,
    businessId: string,
    role = "viewer",
    opts?: { invitePending?: boolean },
  ): Promise<UserBusinessMembership> {
    const invitePending = opts?.invitePending === true;
    const inviteToken = invitePending ? crypto.randomBytes(32).toString("hex") : null;
    const inviteExpiresAt = invitePending ? new Date(Date.now() + 14 * 24 * 60 * 60 * 1000) : null;
    let row: UserBusinessMembership | undefined;
    try {
      [row] = await db.insert(userBusinessMemberships).values({
        id: crypto.randomUUID(),
        userId,
        businessId,
        role,
        membershipStatus: invitePending ? "pending" : "active",
        inviteToken,
        inviteExpiresAt,
      }).onConflictDoNothing().returning();
    } catch (e) {
      if (isMissingRelation(e, "user_business_memberships")) {
        console.warn("[storage] user_business_memberships table missing; cannot link existing user to business");
        throw new Error("Membership linking is unavailable until migrations are applied.");
      }
      throw e;
    }
    if (row) return row;
    let existing: UserBusinessMembership | undefined;
    try {
      [existing] = await db
        .select()
        .from(userBusinessMemberships)
        .where(and(eq(userBusinessMemberships.userId, userId), eq(userBusinessMemberships.businessId, businessId)))
        .limit(1);
    } catch (e) {
      if (isMissingRelation(e, "user_business_memberships")) {
        throw new Error("Membership lookup is unavailable until migrations are applied.");
      }
      throw e;
    }
    if (!existing) throw new Error("Failed to link user to business");

    // Insert conflict: refresh pending invite or bump role — never downgrade active → pending.
    if (invitePending) {
      const st = String(existing.membershipStatus || "active").toLowerCase();
      const hasOpenInvite = !!existing.inviteToken?.trim();
      if (st === "active" && !hasOpenInvite) {
        throw new Error("User is already an active member of this company");
      }
      const newTok = crypto.randomBytes(32).toString("hex");
      const exp = new Date(Date.now() + 14 * 24 * 60 * 60 * 1000);
      await db
        .update(userBusinessMemberships)
        .set({
          membershipStatus: "pending",
          inviteToken: newTok,
          inviteExpiresAt: exp,
          role,
        })
        .where(eq(userBusinessMemberships.id, existing.id));
      const [refreshed] = await db
        .select()
        .from(userBusinessMemberships)
        .where(eq(userBusinessMemberships.id, existing.id))
        .limit(1);
      return refreshed ?? existing;
    }

    if (role && existing.role !== role) {
      await this.setUserBusinessMembershipRole(userId, businessId, role);
      existing.role = role;
    }
    return existing;
  }

  async getMembershipForBusiness(
    userId: string,
    businessId: string,
  ): Promise<{ role: string; status: string; inviteToken: string | null } | null> {
    try {
      const [row] = await db
        .select({
          role: userBusinessMemberships.role,
          status: userBusinessMemberships.membershipStatus,
          inviteToken: userBusinessMemberships.inviteToken,
        })
        .from(userBusinessMemberships)
        .where(and(eq(userBusinessMemberships.userId, userId), eq(userBusinessMemberships.businessId, businessId)))
        .limit(1);
      if (!row) return null;
      return {
        role: row.role,
        status: row.status || "active",
        inviteToken: row.inviteToken ?? null,
      };
    } catch (e) {
      if (isMissingRelation(e, "user_business_memberships")) return null;
      throw e;
    }
  }

  async acceptCompanyInvite(token: string, userId: string): Promise<{ ok: true } | { ok: false; message: string }> {
    try {
      const [row] = await db
        .select()
        .from(userBusinessMemberships)
        .where(eq(userBusinessMemberships.inviteToken, token))
        .limit(1);
      if (!row) return { ok: false, message: "Invite not found." };
      if (row.userId !== userId) return { ok: false, message: "Wrong account for this invite." };
      const st = row.membershipStatus || "active";
      if (st !== "pending") return { ok: false, message: "Invite already accepted." };
      if (row.inviteExpiresAt && new Date(row.inviteExpiresAt).getTime() < Date.now()) {
        return { ok: false, message: "Invite expired." };
      }
      await db
        .update(userBusinessMemberships)
        .set({
          membershipStatus: "active",
          inviteToken: null,
          inviteExpiresAt: null,
        })
        .where(eq(userBusinessMemberships.id, row.id));
      return { ok: true };
    } catch (e) {
      if (isMissingRelation(e, "user_business_memberships")) {
        return { ok: false, message: "Membership invites are unavailable." };
      }
      throw e;
    }
  }

  async getCompanyInvitePreviewByToken(token: string): Promise<{ companyName: string; role: string } | null> {
    try {
      const [row] = await db
        .select({
          businessId: userBusinessMemberships.businessId,
          role: userBusinessMemberships.role,
          status: userBusinessMemberships.membershipStatus,
          expires: userBusinessMemberships.inviteExpiresAt,
        })
        .from(userBusinessMemberships)
        .where(eq(userBusinessMemberships.inviteToken, token))
        .limit(1);
      if (!row) return null;
      if ((row.status || "active") !== "pending") return null;
      if (row.expires && new Date(row.expires).getTime() < Date.now()) return null;
      const biz = await this.getBusinessById(row.businessId);
      if (!biz) return null;
      return { companyName: biz.businessName, role: row.role };
    } catch (e) {
      if (isMissingRelation(e, "user_business_memberships")) return null;
      throw e;
    }
  }

  async getUserBusinessMembershipRole(userId: string, businessId: string): Promise<string | null> {
    try {
      const meta = await this.getMembershipForBusiness(userId, businessId);
      if (!meta || meta.status !== "active") return null;
      return meta.role;
    } catch (e) {
      if (isMissingRelation(e, "user_business_memberships")) return null;
      throw e;
    }
  }

  async setUserBusinessMembershipRole(userId: string, businessId: string, role: string): Promise<void> {
    try {
      await db
        .update(userBusinessMemberships)
        .set({ role })
        .where(and(eq(userBusinessMemberships.userId, userId), eq(userBusinessMemberships.businessId, businessId)));
    } catch (e) {
      if (isMissingRelation(e, "user_business_memberships")) return;
      throw e;
    }
  }

  async userHasBusinessAccess(userId: string, businessId: string): Promise<boolean> {
    const [u] = await db.select({ businessId: users.businessId }).from(users).where(eq(users.id, userId)).limit(1);
    if (u?.businessId === businessId) return true;
    const [owned] = await db.select({ id: businesses.id }).from(businesses).where(and(eq(businesses.id, businessId), eq(businesses.userId, userId))).limit(1);
    if (owned) return true;
    let linked: { id: string; membershipStatus: string } | undefined;
    try {
      [linked] = await db
        .select({
          id: userBusinessMemberships.id,
          membershipStatus: userBusinessMemberships.membershipStatus,
        })
        .from(userBusinessMemberships)
        .where(and(eq(userBusinessMemberships.userId, userId), eq(userBusinessMemberships.businessId, businessId)))
        .limit(1);
    } catch (e) {
      if (isMissingRelation(e, "user_business_memberships")) {
        console.warn("[storage] user_business_memberships table missing; userHasBusinessAccess fallback active");
        return false;
      }
      throw e;
    }
    if (!linked) return false;
    const st = linked.membershipStatus || "active";
    return st === "active";
  }

  async getUsersByRole(role: string): Promise<User[]> {
    return db.select().from(users).where(eq(users.role, role as any)).orderBy(desc(users.createdAt));
  }

  async updateUserRole(id: string, role: string): Promise<User | undefined> {
    const [result] = await db.update(users).set({ role: role as any }).where(eq(users.id, id)).returning();
    return result;
  }

  async updateUserStatus(id: string, status: string): Promise<User | undefined> {
    const [result] = await db.update(users).set({ status }).where(eq(users.id, id)).returning();
    return result;
  }

  async updateUserBusinessId(id: string, businessId: string): Promise<User | undefined> {
    const [result] = await db.update(users).set({ businessId }).where(eq(users.id, id)).returning();
    return result;
  }

  async updateUserProfileImage(id: string, profileImageUrl: string | null): Promise<User | undefined> {
    const [result] = await db.update(users).set({ profileImageUrl }).where(eq(users.id, id)).returning();
    return result;
  }

  async updateUserProfile(id: string, data: { fullName?: string; phone?: string | null }): Promise<User | undefined> {
    const [result] = await db
      .update(users)
      .set({
        ...(typeof data.fullName === "string" ? { fullName: data.fullName } : {}),
        ...(data.phone !== undefined ? { phone: data.phone } : {}),
      })
      .where(eq(users.id, id))
      .returning();
    return result;
  }

  async updateUserGoogleId(id: string, googleId: string): Promise<User | undefined> {
    const [result] = await db
      .update(users)
      .set({ googleId, emailVerifiedAt: sql`COALESCE(${users.emailVerifiedAt}, NOW())` })
      .where(eq(users.id, id))
      .returning();
    return result;
  }

  async setEmailVerificationToken(userId: string, token: string, expiresAt: Date): Promise<User | undefined> {
    const [result] = await db
      .update(users)
      .set({
        emailVerificationToken: token,
        emailVerificationExpiresAt: expiresAt,
      })
      .where(eq(users.id, userId))
      .returning();
    return result;
  }

  async getUserByEmailVerificationToken(token: string): Promise<User | undefined> {
    const [result] = await db.select().from(users).where(
      and(eq(users.emailVerificationToken, token), gt(users.emailVerificationExpiresAt, new Date()))
    );
    return result;
  }

  async markUserEmailVerified(userId: string): Promise<User | undefined> {
    const [result] = await db
      .update(users)
      .set({
        emailVerifiedAt: new Date(),
        emailVerificationToken: null,
        emailVerificationExpiresAt: null,
        status: "active",
      })
      .where(eq(users.id, userId))
      .returning();
    return result;
  }

  async verifyPassword(password: string, hash: string): Promise<boolean> {
    return bcrypt.compare(password, hash);
  }

  async setPasswordResetToken(email: string, token: string, expiresAt: Date): Promise<User | undefined> {
    const [result] = await db.update(users).set({
      resetToken: token,
      resetTokenExpiresAt: expiresAt,
    }).where(eq(users.email, email)).returning();
    return result;
  }

  async getUserByResetToken(token: string): Promise<User | undefined> {
    const [result] = await db.select().from(users).where(
      and(eq(users.resetToken, token), gt(users.resetTokenExpiresAt, new Date()))
    );
    return result;
  }

  async updateUserPassword(userId: string, newPassword: string): Promise<User | undefined> {
    const passwordHash = await bcrypt.hash(newPassword, 10);
    const [result] = await db.update(users).set({
      passwordHash,
      resetToken: null,
      resetTokenExpiresAt: null,
    }).where(eq(users.id, userId)).returning();
    return result;
  }

  async updateUserEmail(userId: string, newEmail: string): Promise<User | undefined> {
    const [result] = await db.update(users).set({
      email: newEmail.toLowerCase().trim(),
      emailVerifiedAt: sql`NOW()`,
    }).where(eq(users.id, userId)).returning();
    return result;
  }

  // ──────────── Sessions ────────────
  async createSession(userId: string): Promise<Session> {
    return withRetry("createSession", async () => {
      const token = crypto.randomBytes(32).toString("hex");
      const expiresAt = new Date(Date.now() + 7 * 24 * 60 * 60 * 1000);
      const [result] = await db.insert(sessions).values({ id: crypto.randomUUID(), userId, token, expiresAt }).returning();
      return result;
    });
  }

  async getSessionByToken(token: string): Promise<Session | undefined> {
    return withRetry("getSessionByToken", async () => {
      const [result] = await db.select().from(sessions).where(
        and(eq(sessions.token, token), gt(sessions.expiresAt, new Date()))
      );
      return result;
    });
  }

  /** Extend session TTL by 7 days from now. Returns updated session or undefined. */
  async extendSession(token: string): Promise<Session | undefined> {
    return withRetry("extendSession", async () => {
      const expiresAt = new Date(Date.now() + 7 * 24 * 60 * 60 * 1000);
      const [result] = await db.update(sessions).set({ expiresAt }).where(eq(sessions.token, token)).returning();
      return result;
    });
  }

  async deleteSession(token: string): Promise<void> {
    await withRetry("deleteSession", async () => {
      await db.delete(sessions).where(eq(sessions.token, token));
    });
  }

  async deleteUserSessions(userId: string): Promise<void> {
    await db.delete(sessions).where(eq(sessions.userId, userId));
  }

  async setImpersonatedBusiness(token: string, businessId: string | null): Promise<Session | undefined> {
    const [result] = await db.update(sessions).set({ impersonatedBusinessId: businessId }).where(eq(sessions.token, token)).returning();
    return result;
  }

  async setImpersonatedUser(token: string, userId: string | null): Promise<Session | undefined> {
    const [result] = await db.update(sessions).set({ impersonatedUserId: userId }).where(eq(sessions.token, token)).returning();
    return result;
  }

  // ──────────── Business KYC ────────────
  async createBusiness(business: InsertBusiness): Promise<Business> {
    const [result] = await db.insert(businesses).values({ ...business, id: crypto.randomUUID() }).returning();
    return result;
  }

  async getBusinessByUserId(userId: string): Promise<Business | undefined> {
    return withRetry("getBusinessByUserId", async () => {
      const [ownerBusiness] = await db.select().from(businesses).where(eq(businesses.userId, userId));
      if (ownerBusiness) return ownerBusiness;

      const [user] = await db
        .select({ businessId: users.businessId, createdBy: users.createdBy })
        .from(users)
        .where(eq(users.id, userId));

      if (user?.businessId) {
        const [memberBusiness] = await db.select().from(businesses).where(eq(businesses.id, user.businessId));
        if (memberBusiness) return memberBusiness;
      }

      // Legacy fallback: team member without business_id inherits creator's company.
      if (user?.createdBy) {
        const [creator] = await db
          .select({ businessId: users.businessId })
          .from(users)
          .where(eq(users.id, user.createdBy));

        if (creator?.businessId) {
          const [creatorBusiness] = await db.select().from(businesses).where(eq(businesses.id, creator.businessId));
          if (creatorBusiness) return creatorBusiness;
        }

        const [creatorOwnedBusiness] = await db.select().from(businesses).where(eq(businesses.userId, user.createdBy));
        if (creatorOwnedBusiness) return creatorOwnedBusiness;
      }

      return undefined;
    });
  }

  async getHrErpCredentials(userId: string): Promise<{
    frappeApiKey: string;
    frappeApiSecret: string;
    companyOverride: string | null;
  } | null> {
    const [row] = await db.select().from(hrErpCredentials).where(eq(hrErpCredentials.userId, userId)).limit(1);
    if (!row) return null;
    return {
      frappeApiKey: row.frappeApiKey,
      frappeApiSecret: row.frappeApiSecret,
      companyOverride: row.companyOverride,
    };
  }

  async getCentyposErpCredentials(userId: string): Promise<{
    frappeApiKey: string;
    frappeApiSecret: string;
    companyOverride: string | null;
  } | null> {
    const [row] = await db.select().from(centyposErpCredentials).where(eq(centyposErpCredentials.userId, userId)).limit(1);
    if (!row) return null;
    return {
      frappeApiKey: row.frappeApiKey,
      frappeApiSecret: row.frappeApiSecret,
      companyOverride: row.companyOverride,
    };
  }

  async upsertCentyposErpCredentials(
    userId: string,
    frappeApiKey: string,
    frappeApiSecret: string,
    companyOverride?: string | null,
  ): Promise<void> {
    const now = new Date();
    await db
      .insert(centyposErpCredentials)
      .values({
        userId,
        frappeApiKey,
        frappeApiSecret,
        companyOverride: companyOverride ?? null,
        updatedAt: now,
      })
      .onConflictDoUpdate({
        target: centyposErpCredentials.userId,
        set: {
          frappeApiKey,
          frappeApiSecret,
          companyOverride: companyOverride ?? null,
          updatedAt: now,
        },
      });
  }

  async deleteCentyposErpCredentials(userId: string): Promise<void> {
    await db.delete(centyposErpCredentials).where(eq(centyposErpCredentials.userId, userId));
  }

  async createHrEmployeeDraft(
    userId: string,
    payload: Record<string, unknown>,
    opts?: { status?: string }
  ): Promise<HrEmployeeDraft> {
    const status = String(opts?.status ?? "").trim();
    const values = status ? ({ userId, payload, status } as const) : ({ userId, payload } as const);
    const [row] = await db.insert(hrEmployeeDrafts).values(values as any).returning();
    return row!;
  }

  async getHrEmployeeDraftById(id: string): Promise<HrEmployeeDraft | undefined> {
    const [row] = await db.select().from(hrEmployeeDrafts).where(eq(hrEmployeeDrafts.id, id)).limit(1);
    return row;
  }

  async claimNextHrEmployeeDraft(): Promise<HrEmployeeDraft | null> {
    const pendingStatus = String(process.env.HR_EMPLOYEE_DRAFT_PENDING_STATUS ?? "pending").trim() || "pending";
    const result = await withRetry("claimNextHrEmployeeDraft", () =>
      pool.query<{ id: string }>(
        `UPDATE hr_employee_drafts SET status = 'processing', updated_at = now()
         WHERE id = (
           SELECT id FROM hr_employee_drafts WHERE status = $1 ORDER BY created_at ASC LIMIT 1 FOR UPDATE SKIP LOCKED
         )
         RETURNING id`,
        [pendingStatus]
      )
    );
    const id = result.rows[0]?.id;
    if (!id) return null;
    return (await this.getHrEmployeeDraftById(id)) ?? null;
  }

  async completeHrEmployeeDraft(id: string, erpEmployeeName: string): Promise<void> {
    await db
      .update(hrEmployeeDrafts)
      .set({
        status: "completed",
        erpEmployeeName,
        error: null,
        updatedAt: new Date(),
      })
      .where(eq(hrEmployeeDrafts.id, id));
  }

  async failHrEmployeeDraft(id: string, error: string): Promise<void> {
    const trimmed = error.trim().slice(0, 8000);
    await db
      .update(hrEmployeeDrafts)
      .set({
        status: "failed",
        error: trimmed,
        updatedAt: new Date(),
      })
      .where(eq(hrEmployeeDrafts.id, id));
  }

  async createHrEmployeeInvite(params: {
    email: string;
    companyKey: string;
    invitedByEmail: string;
    expiresInDays?: number;
  }): Promise<HrEmployeeInvite> {
    const days = Math.min(30, Math.max(1, params.expiresInDays ?? 14));
    const expiresAt = new Date(Date.now() + days * 86_400_000);
    const token = crypto.randomBytes(32).toString("hex");
    const [row] = await db
      .insert(hrEmployeeInvites)
      .values({
        token,
        email: params.email.trim().toLowerCase(),
        companyKey: params.companyKey,
        invitedByEmail: params.invitedByEmail,
        status: "pending",
        expiresAt,
      })
      .returning();
    return row;
  }

  async getHrEmployeeInviteByToken(token: string): Promise<HrEmployeeInvite | undefined> {
    const [row] = await db
      .select()
      .from(hrEmployeeInvites)
      .where(eq(hrEmployeeInvites.token, token))
      .limit(1);
    return row;
  }

  async markHrEmployeeInviteCompleted(token: string): Promise<void> {
    await db
      .update(hrEmployeeInvites)
      .set({ status: "completed" })
      .where(eq(hrEmployeeInvites.token, token));
  }

  async createHrInfoRequest(params: {
    employeeId: string;
    employeeName: string;
    employeeEmail: string;
    adminEmail: string;
    adminUserId: string;
    companyKey: string;
    requestedFields: { fieldname: string; label: string }[];
    expiresInDays?: number;
  }): Promise<typeof hrInfoRequests.$inferSelect> {
    const days = Math.min(30, Math.max(1, params.expiresInDays ?? 7));
    const expiresAt = new Date(Date.now() + days * 86_400_000);
    const token = (await import("crypto")).randomBytes(32).toString("hex");
    const [row] = await db
      .insert(hrInfoRequests)
      .values({
        id: crypto.randomUUID(),
        token,
        employeeId: params.employeeId,
        employeeName: params.employeeName,
        employeeEmail: params.employeeEmail,
        adminEmail: params.adminEmail,
        adminUserId: params.adminUserId,
        companyKey: params.companyKey,
        requestedFields: params.requestedFields,
        status: "pending",
        expiresAt,
      })
      .returning();
    return row;
  }

  async getHrInfoRequestByToken(token: string): Promise<typeof hrInfoRequests.$inferSelect | undefined> {
    const [row] = await db
      .select()
      .from(hrInfoRequests)
      .where(eq(hrInfoRequests.token, token))
      .limit(1);
    return row;
  }

  async updateHrInfoRequestSubmittedValues(token: string, submitted: Record<string, string>): Promise<void> {
    const row = await this.getHrInfoRequestByToken(token);
    if (!row) return;
    const existing = Array.isArray(row.requestedFields) ? row.requestedFields : [];
    const enriched = existing.map((f) => {
      const fieldname = String((f as Record<string, unknown>).fieldname ?? "").trim();
      const value = submitted[fieldname];
      if (!value) return f;
      return {
        ...(f as Record<string, unknown>),
        submitted_value: value,
        submitted_at: new Date().toISOString(),
      };
    });
    await db
      .update(hrInfoRequests)
      .set({ requestedFields: enriched as { fieldname: string; label: string }[] })
      .where(eq(hrInfoRequests.token, token));
  }

  async markHrInfoRequestCompleted(token: string): Promise<void> {
    await db
      .update(hrInfoRequests)
      .set({ status: "completed" })
      .where(eq(hrInfoRequests.token, token));
  }

  async markHrInfoRequestSubmitted(token: string): Promise<void> {
    await db
      .update(hrInfoRequests)
      .set({ status: "submitted" })
      .where(eq(hrInfoRequests.token, token));
  }

  async getLatestCompletedHrInfoRequestByEmployee(employeeId: string): Promise<typeof hrInfoRequests.$inferSelect | undefined> {
    const [row] = await db
      .select()
      .from(hrInfoRequests)
      .where(and(
        eq(hrInfoRequests.employeeId, employeeId),
        or(eq(hrInfoRequests.status, "completed"), eq(hrInfoRequests.status, "approved")),
      ))
      .orderBy(desc(hrInfoRequests.createdAt))
      .limit(1);
    return row;
  }

  async listHrInfoRequestsByCompany(companyKey: string, status?: string): Promise<(typeof hrInfoRequests.$inferSelect)[]> {
    const where = status
      ? and(eq(hrInfoRequests.companyKey, companyKey), eq(hrInfoRequests.status, status))
      : eq(hrInfoRequests.companyKey, companyKey);
    return db
      .select()
      .from(hrInfoRequests)
      .where(where)
      .orderBy(desc(hrInfoRequests.createdAt));
  }

  async getHrInfoRequestById(id: string): Promise<typeof hrInfoRequests.$inferSelect | undefined> {
    const [row] = await db
      .select()
      .from(hrInfoRequests)
      .where(eq(hrInfoRequests.id, id))
      .limit(1);
    return row;
  }

  async reviewHrInfoRequestById(
    id: string,
    status: "approved" | "rejected",
    remarks: string,
    reviewedByEmail: string,
  ): Promise<typeof hrInfoRequests.$inferSelect | undefined> {
    const row = await this.getHrInfoRequestById(id);
    if (!row) return undefined;
    const existing = Array.isArray(row.requestedFields) ? row.requestedFields : [];
    const reviewedAt = new Date().toISOString();
    const enriched = existing.map((f) => ({
      ...(f as Record<string, unknown>),
      review_status: status,
      review_remarks: remarks,
      reviewed_by: reviewedByEmail,
      reviewed_at: reviewedAt,
    }));
    const [updated] = await db
      .update(hrInfoRequests)
      .set({ status, requestedFields: enriched as { fieldname: string; label: string }[] })
      .where(eq(hrInfoRequests.id, id))
      .returning();
    return updated;
  }

  async deleteHrOrgUnit(
    businessId: string,
    kind: "department" | "designation" | "branch",
    value: string,
  ): Promise<boolean> {
    const lower = value.trim().toLowerCase();
    const rows = await db
      .select()
      .from(hrOrgUnits)
      .where(and(eq(hrOrgUnits.businessId, businessId), eq(hrOrgUnits.kind, kind), eq(hrOrgUnits.isActive, true)));
    const match = rows.find((r) => r.value.trim().toLowerCase() === lower);
    if (!match) return false;
    await db.update(hrOrgUnits).set({ isActive: false, updatedAt: new Date() }).where(eq(hrOrgUnits.id, match.id));
    return true;
  }

  async listHrOrgUnits(
    businessId: string
  ): Promise<{ department: HrOrgUnit[]; designation: HrOrgUnit[]; branch: HrOrgUnit[] }> {
    const rows = await db
      .select()
      .from(hrOrgUnits)
      .where(and(eq(hrOrgUnits.businessId, businessId), eq(hrOrgUnits.isActive, true)))
      .orderBy(asc(hrOrgUnits.kind), asc(hrOrgUnits.value));
    return {
      department: rows.filter((r) => r.kind === "department"),
      designation: rows.filter((r) => r.kind === "designation"),
      branch: rows.filter((r) => r.kind === "branch"),
    };
  }

  async renameHrOrgUnit(
    businessId: string,
    kind: "department" | "designation" | "branch",
    oldValue: string,
    newValue: string,
  ): Promise<HrOrgUnit | null> {
    const oldLower = oldValue.trim().toLowerCase();
    const newTrimmed = newValue.trim();
    const rows = await db
      .select()
      .from(hrOrgUnits)
      .where(and(eq(hrOrgUnits.businessId, businessId), eq(hrOrgUnits.kind, kind), eq(hrOrgUnits.isActive, true)));
    const match = rows.find((r) => r.value.trim().toLowerCase() === oldLower);
    if (!match) return null;
    const [updated] = await db
      .update(hrOrgUnits)
      .set({ value: newTrimmed, updatedAt: new Date() })
      .where(eq(hrOrgUnits.id, match.id))
      .returning();
    return updated ?? null;
  }

  async upsertHrOrgUnit(
    businessId: string,
    kind: "department" | "designation" | "branch",
    value: string,
    userId: string
  ): Promise<HrOrgUnit> {
    const trimmed = value.trim();
    const lower = trimmed.toLowerCase();
    const existing = await db
      .select()
      .from(hrOrgUnits)
      .where(and(eq(hrOrgUnits.businessId, businessId), eq(hrOrgUnits.kind, kind)));
    const match = existing.find((r) => r.value.trim().toLowerCase() === lower);
    if (match) {
      const [row] = await db
        .update(hrOrgUnits)
        .set({ value: trimmed, isActive: true, updatedAt: new Date() })
        .where(eq(hrOrgUnits.id, match.id))
        .returning();
      return row!;
    }
    const [row] = await db
      .insert(hrOrgUnits)
      .values({ businessId, kind, value: trimmed, isActive: true, createdBy: userId })
      .returning();
    return row!;
  }

  async listHrLifecycleEvents(businessId: string, employeeId: string): Promise<HrLifecycleEvent[]> {
    return db
      .select()
      .from(hrLifecycleEvents)
      .where(and(eq(hrLifecycleEvents.businessId, businessId), eq(hrLifecycleEvents.employeeId, employeeId)))
      .orderBy(desc(hrLifecycleEvents.createdAt));
  }

  async getHrLifecycleEventById(id: string): Promise<HrLifecycleEvent | undefined> {
    const [row] = await db.select().from(hrLifecycleEvents).where(eq(hrLifecycleEvents.id, id)).limit(1);
    return row;
  }

  async updateHrLifecycleEventStatus(
    id: string,
    status: "draft" | "submitted" | "approved" | "rejected" | "completed",
    notes?: string | null
  ): Promise<HrLifecycleEvent | undefined> {
    const [row] = await db
      .update(hrLifecycleEvents)
      .set({
        status,
        notes: notes === undefined ? sql`${hrLifecycleEvents.notes}` : notes ?? null,
        updatedAt: new Date(),
      })
      .where(eq(hrLifecycleEvents.id, id))
      .returning();
    return row;
  }

  async createHrLifecycleEvent(input: {
    businessId: string;
    employeeId: string;
    eventType: "promotion" | "transfer" | "offboarding_checklist" | "exit_interview";
    effectiveDate?: string | null;
    status?: string | null;
    notes?: string | null;
    payload?: Record<string, unknown>;
    userId: string;
  }): Promise<HrLifecycleEvent> {
    const [row] = await db
      .insert(hrLifecycleEvents)
      .values({
        businessId: input.businessId,
        employeeId: input.employeeId,
        eventType: input.eventType,
        effectiveDate: input.effectiveDate ?? null,
        status: input.status ?? "draft",
        notes: input.notes ?? null,
        payload: input.payload ?? {},
        createdBy: input.userId,
      })
      .returning();
    return row!;
  }

  async listHrOnboardingTemplates(
    businessId: string,
    filters?: { department?: string; designation?: string; activeOnly?: boolean }
  ): Promise<HrOnboardingTemplate[]> {
    const clauses = [eq(hrOnboardingTemplates.businessId, businessId)];
    if (filters?.activeOnly !== false) clauses.push(eq(hrOnboardingTemplates.isActive, true));
    if (filters?.department) clauses.push(eq(hrOnboardingTemplates.department, filters.department));
    if (filters?.designation) clauses.push(eq(hrOnboardingTemplates.designation, filters.designation));
    return db
      .select()
      .from(hrOnboardingTemplates)
      .where(and(...clauses))
      .orderBy(desc(hrOnboardingTemplates.createdAt));
  }

  async createHrOnboardingTemplate(input: {
    businessId: string;
    name: string;
    department?: string | null;
    designation?: string | null;
    tasks: Array<{ title: string; description?: string; dueDays?: number }>;
    userId: string;
  }): Promise<HrOnboardingTemplate> {
    const [row] = await db
      .insert(hrOnboardingTemplates)
      .values({
        businessId: input.businessId,
        name: input.name.trim(),
        department: input.department?.trim() || null,
        designation: input.designation?.trim() || null,
        tasks: input.tasks,
        createdBy: input.userId,
        isActive: true,
      })
      .returning();
    return row!;
  }

  async listHrOnboardingTasks(businessId: string, employeeId: string): Promise<HrOnboardingTask[]> {
    return db
      .select()
      .from(hrOnboardingTasks)
      .where(and(eq(hrOnboardingTasks.businessId, businessId), eq(hrOnboardingTasks.employeeId, employeeId)))
      .orderBy(desc(hrOnboardingTasks.createdAt));
  }

  async createHrOnboardingTasksFromTemplate(input: {
    businessId: string;
    employeeId: string;
    templateId: string;
    assignedTo?: string | null;
    userId: string;
  }): Promise<HrOnboardingTask[]> {
    const [template] = await db
      .select()
      .from(hrOnboardingTemplates)
      .where(and(eq(hrOnboardingTemplates.id, input.templateId), eq(hrOnboardingTemplates.businessId, input.businessId)))
      .limit(1);
    if (!template) return [];
    const today = new Date();
    const tasks = Array.isArray(template.tasks) ? template.tasks : [];
    if (!tasks.length) return [];
    const values = tasks
      .map((t) => {
        const title = String(t?.title ?? "").trim();
        if (!title) return null;
        const dueDays = Number(t?.dueDays ?? 0);
        const dueDate = Number.isFinite(dueDays) && dueDays > 0
          ? new Date(today.getTime() + dueDays * 24 * 60 * 60 * 1000).toISOString().slice(0, 10)
          : null;
        return {
          businessId: input.businessId,
          employeeId: input.employeeId,
          templateId: template.id,
          title,
          description: t?.description ? String(t.description) : null,
          status: "pending",
          dueDate,
          assignedTo: input.assignedTo?.trim() || null,
          createdBy: input.userId,
        };
      })
      .filter((v): v is NonNullable<typeof v> => !!v);
    if (!values.length) return [];
    return db.insert(hrOnboardingTasks).values(values).returning();
  }

  async updateHrOnboardingTask(
    id: string,
    patch: {
      status?: "pending" | "in_progress" | "completed" | "blocked";
      dueDate?: string | null;
      assignedTo?: string | null;
      notes?: string | null;
    }
  ): Promise<HrOnboardingTask | undefined> {
    const set: Record<string, unknown> = { updatedAt: new Date() };
    if (patch.status) set.status = patch.status;
    if (patch.dueDate !== undefined) set.dueDate = patch.dueDate;
    if (patch.assignedTo !== undefined) set.assignedTo = patch.assignedTo;
    if (patch.notes !== undefined) set.notes = patch.notes;
    if (patch.status === "completed") set.completedAt = new Date();
    if (patch.status && patch.status !== "completed") set.completedAt = null;
    const [row] = await db
      .update(hrOnboardingTasks)
      .set(set)
      .where(eq(hrOnboardingTasks.id, id))
      .returning();
    return row;
  }

  // ── Teams ──────────────────────────────────────────────────────────────

  async listTeams(businessId: string): Promise<Team[]> {
    return db.select().from(teams)
      .where(and(eq(teams.businessId, businessId), eq(teams.isActive, true)))
      .orderBy(asc(teams.name));
  }

  async getTeamById(id: string): Promise<Team | undefined> {
    const [row] = await db.select().from(teams).where(eq(teams.id, id));
    return row;
  }

  async createTeam(input: { businessId: string; name: string; description?: string | null; color?: string | null; leadUserId?: string | null; createdBy: string }): Promise<Team> {
    const [row] = await db.insert(teams).values({
      businessId: input.businessId,
      name: input.name.trim(),
      description: input.description?.trim() || null,
      color: input.color || null,
      leadUserId: input.leadUserId || null,
      isActive: true,
      createdBy: input.createdBy,
    }).returning();
    return row;
  }

  async updateTeam(id: string, patch: { name?: string; description?: string | null; color?: string | null; leadUserId?: string | null; isActive?: boolean }): Promise<Team | undefined> {
    const set: Partial<InsertTeam> & { updatedAt: Date } = { updatedAt: new Date() };
    if (patch.name !== undefined) set.name = patch.name.trim();
    if (patch.description !== undefined) set.description = patch.description?.trim() || null;
    if (patch.color !== undefined) set.color = patch.color || null;
    if (patch.leadUserId !== undefined) set.leadUserId = patch.leadUserId || null;
    if (patch.isActive !== undefined) set.isActive = patch.isActive;
    const [row] = await db.update(teams).set(set).where(eq(teams.id, id)).returning();
    return row;
  }

  async deleteTeam(id: string): Promise<boolean> {
    const [row] = await db.update(teams).set({ isActive: false, updatedAt: new Date() }).where(eq(teams.id, id)).returning();
    return !!row;
  }

  async listTeamMembers(teamId: string): Promise<(TeamMember & { user: Pick<User, "id" | "fullName" | "email" | "phone" | "profileImageUrl" | "role"> })[]> {
    const rows = await db
      .select({
        id: teamMembers.id,
        teamId: teamMembers.teamId,
        businessId: teamMembers.businessId,
        userId: teamMembers.userId,
        role: teamMembers.role,
        joinedAt: teamMembers.joinedAt,
        user: {
          id: users.id,
          fullName: users.fullName,
          email: users.email,
          phone: users.phone,
          profileImageUrl: users.profileImageUrl,
          role: users.role,
        },
      })
      .from(teamMembers)
      .innerJoin(users, eq(teamMembers.userId, users.id))
      .where(eq(teamMembers.teamId, teamId))
      .orderBy(asc(users.fullName));
    return rows as any;
  }

  async addTeamMember(input: { teamId: string; businessId: string; userId: string; role?: string | null }): Promise<TeamMember> {
    const [existing] = await db.select().from(teamMembers)
      .where(and(eq(teamMembers.teamId, input.teamId), eq(teamMembers.userId, input.userId)));
    if (existing) return existing;
    const [row] = await db.insert(teamMembers).values({
      teamId: input.teamId,
      businessId: input.businessId,
      userId: input.userId,
      role: input.role || null,
    }).returning();
    return row;
  }

  async removeTeamMember(teamId: string, userId: string): Promise<boolean> {
    const result = await db.delete(teamMembers)
      .where(and(eq(teamMembers.teamId, teamId), eq(teamMembers.userId, userId)));
    return (result.rowCount ?? 0) > 0;
  }

  async getTeamsByUserId(businessId: string, userId: string): Promise<Team[]> {
    const memberRows = await db.select({ teamId: teamMembers.teamId })
      .from(teamMembers)
      .where(and(eq(teamMembers.businessId, businessId), eq(teamMembers.userId, userId)));
    const ids = memberRows.map(r => r.teamId);
    if (!ids.length) return [];
    return db.select().from(teams)
      .where(and(inArray(teams.id, ids), eq(teams.isActive, true)))
      .orderBy(asc(teams.name));
  }

  async getBusinessById(id: string): Promise<Business | undefined> {
    return withRetry("getBusinessById", async () => {
      const [result] = await db.select().from(businesses).where(eq(businesses.id, id));
      return result;
    });
  }

  async getBusinessByAccountNumber(accountNumber: string): Promise<Business | undefined> {
    return withRetry("getBusinessByAccountNumber", async () => {
      const [result] = await db.select().from(businesses).where(eq(businesses.accountNumber, accountNumber));
      return result;
    });
  }

  async getBusinessesByName(name: string): Promise<Business[]> {
    if (!name.trim()) return [];
    const pattern = `%${name.trim()}%`;
    return db.select().from(businesses).where(ilike(businesses.businessName, pattern)).orderBy(desc(businesses.createdAt));
  }

  async updateBusinessKyc(id: string, data: Partial<InsertBusiness>): Promise<Business | undefined> {
    const [result] = await db.update(businesses).set(data).where(eq(businesses.id, id)).returning();
    return result;
  }

  async updateBusinessLogoUrl(id: string, companyLogoUrl: string): Promise<Business | undefined> {
    const [result] = await db.update(businesses).set({ companyLogoUrl }).where(eq(businesses.id, id)).returning();
    return result;
  }

  async updateBusinessUserId(id: string, userId: string): Promise<Business | undefined> {
    const [result] = await db.update(businesses).set({ userId }).where(eq(businesses.id, id)).returning();
    return result;
  }

  async updateBusinessCentyPackOrg(
    id: string,
    patch: { parentBusinessId?: string | null; centypackDisabled?: boolean; centypackBetaEnabled?: boolean },
  ): Promise<Business | undefined> {
    const setClause: {
      parentBusinessId?: string | null;
      centypackDisabled?: boolean;
      centypackBetaEnabled?: boolean;
    } = {};
    if (patch.parentBusinessId !== undefined) setClause.parentBusinessId = patch.parentBusinessId;
    if (patch.centypackDisabled !== undefined) setClause.centypackDisabled = patch.centypackDisabled;
    if (patch.centypackBetaEnabled !== undefined) setClause.centypackBetaEnabled = patch.centypackBetaEnabled;
    if (Object.keys(setClause).length === 0) {
      return this.getBusinessById(id);
    }
    const [result] = await db.update(businesses).set(setClause).where(eq(businesses.id, id)).returning();
    return result;
  }

  async updateBusinessCentyPackHubHiddenTiles(id: string, hiddenTileIds: string[]): Promise<Business | undefined> {
    const normalized = [...new Set(hiddenTileIds.map((s) => String(s).trim().toLowerCase()).filter(Boolean))];
    const [result] = await db
      .update(businesses)
      .set({ centypackHubHiddenTileIds: normalized })
      .where(eq(businesses.id, id))
      .returning();
    return result;
  }

  async createCentyPackMirrorSyncLog(input: {
    businessId: string;
    businessName?: string | null;
    source: string;
    endpoint: string;
    attempted: boolean;
    ok: boolean;
    statusCode?: number | null;
    error?: string | null;
    payload?: Record<string, unknown> | null;
    retryCount?: number;
    nextRetryAt?: Date | null;
  }): Promise<CentypackMirrorSyncLog> {
    const [row] = await db
      .insert(centypackMirrorSyncLogs)
      .values({
        businessId: input.businessId,
        businessName: input.businessName ?? null,
        source: input.source,
        endpoint: input.endpoint,
        attempted: input.attempted,
        ok: input.ok,
        statusCode: input.statusCode ?? null,
        error: input.error ?? null,
        payload: input.payload ?? null,
        retryCount: input.retryCount ?? 0,
        nextRetryAt: input.nextRetryAt ?? null,
      })
      .returning();
    return row;
  }

  async listCentyPackMirrorSyncLogs(businessId: string, limit = 20): Promise<CentypackMirrorSyncLog[]> {
    return db
      .select()
      .from(centypackMirrorSyncLogs)
      .where(eq(centypackMirrorSyncLogs.businessId, businessId))
      .orderBy(desc(centypackMirrorSyncLogs.createdAt))
      .limit(Math.max(1, Math.min(limit, 100)));
  }

  async listPendingCentyPackMirrorRetries(limit = 50): Promise<CentypackMirrorSyncLog[]> {
    return db
      .select()
      .from(centypackMirrorSyncLogs)
      .where(
        and(
          eq(centypackMirrorSyncLogs.ok, false),
          isNotNull(centypackMirrorSyncLogs.nextRetryAt),
          lte(centypackMirrorSyncLogs.nextRetryAt, new Date()),
        ),
      )
      .orderBy(asc(centypackMirrorSyncLogs.nextRetryAt), desc(centypackMirrorSyncLogs.createdAt))
      .limit(Math.max(1, Math.min(limit, 200)));
  }

  async markCentyPackMirrorSyncLogResult(
    id: string,
    patch: { ok: boolean; statusCode?: number | null; error?: string | null; retryCount?: number; nextRetryAt?: Date | null },
  ): Promise<CentypackMirrorSyncLog | undefined> {
    const [row] = await db
      .update(centypackMirrorSyncLogs)
      .set({
        ok: patch.ok,
        statusCode: patch.statusCode ?? null,
        error: patch.error ?? null,
        retryCount: patch.retryCount,
        nextRetryAt: patch.nextRetryAt ?? null,
        updatedAt: new Date(),
      })
      .where(eq(centypackMirrorSyncLogs.id, id))
      .returning();
    return row;
  }

  async listLaborWorkers(businessId: string): Promise<LaborWorker[]> {
    return db
      .select()
      .from(laborWorkers)
      .where(eq(laborWorkers.businessId, businessId))
      .orderBy(desc(laborWorkers.active), asc(laborWorkers.fullName));
  }

  async createLaborWorker(input: InsertLaborWorker): Promise<LaborWorker> {
    const [row] = await db.insert(laborWorkers).values(input).returning();
    return row;
  }

  async listLaborPieceRates(businessId: string): Promise<LaborPieceRate[]> {
    return db
      .select()
      .from(laborPieceRates)
      .where(eq(laborPieceRates.businessId, businessId))
      .orderBy(desc(laborPieceRates.effectiveFrom), desc(laborPieceRates.createdAt));
  }

  async createLaborPieceRate(input: InsertLaborPieceRate): Promise<LaborPieceRate> {
    const [row] = await db.insert(laborPieceRates).values(input).returning();
    return row;
  }

  async listLaborDailyEntries(
    businessId: string,
    fromDate: string,
    toDate: string,
  ): Promise<Array<LaborDailyEntry & { workerName: string }>> {
    const rows = await db
      .select({
        entry: laborDailyEntries,
        workerName: laborWorkers.fullName,
      })
      .from(laborDailyEntries)
      .innerJoin(laborWorkers, eq(laborWorkers.id, laborDailyEntries.workerId))
      .where(
        and(
          eq(laborDailyEntries.businessId, businessId),
          gte(laborDailyEntries.workDate, fromDate),
          lte(laborDailyEntries.workDate, toDate),
        ),
      )
      .orderBy(desc(laborDailyEntries.workDate), asc(laborWorkers.fullName));
    return rows.map((r) => ({ ...r.entry, workerName: r.workerName }));
  }

  async createLaborDailyEntry(input: InsertLaborDailyEntry): Promise<LaborDailyEntry> {
    const [row] = await db.insert(laborDailyEntries).values(input).returning();
    return row;
  }

  async createLaborPayrollRun(input: InsertLaborPayrollRun): Promise<LaborPayrollRun> {
    const [row] = await db.insert(laborPayrollRuns).values(input).returning();
    return row;
  }

  async createLaborPayrollLines(lines: InsertLaborPayrollLine[]): Promise<LaborPayrollLine[]> {
    if (!lines.length) return [];
    return db.insert(laborPayrollLines).values(lines).returning();
  }

  async listLaborPayrollRuns(businessId: string, limit = 12): Promise<LaborPayrollRun[]> {
    return db
      .select()
      .from(laborPayrollRuns)
      .where(eq(laborPayrollRuns.businessId, businessId))
      .orderBy(desc(laborPayrollRuns.createdAt))
      .limit(Math.max(1, Math.min(limit, 52)));
  }

  /** Two letters from name (e.g. "Haron Enterprise" -> HE, "Dennis Cheruyot" -> DC). */
  private twoLettersFromName(name: string): string {
    const words = name.trim().split(/\s+/).filter(Boolean);
    const initials =
      words.length >= 2
        ? (words[0][0] ?? "X") + (words[1][0] ?? "X")
        : name.trim().slice(0, 2) || "XX";
    return initials
      .toUpperCase()
      .replace(/[^A-Z]/g, "X")
      .slice(0, 2)
      .padEnd(2, "X");
  }

  /** Next 4-digit sequence value; single sequence for both business and customer accounts to ensure uniqueness. */
  private async nextAccountNumberSequence(): Promise<number> {
    const seqResult = await pool.query<{ next_val: number }>(
      "UPDATE account_number_sequence SET next_val = next_val + 1 WHERE id = 1 RETURNING next_val"
    );
    return seqResult.rows[0]?.next_val ?? 1;
  }

  /** Initials for employee wallet number prefix, fallback to XX. */
  private twoLettersFromInitials(name: string): string {
    const parts = String(name || "")
      .trim()
      .split(/\s+/)
      .filter(Boolean);
    const first = (parts[0]?.[0] || "").toUpperCase().replace(/[^A-Z]/g, "");
    const second = (parts[1]?.[0] || "").toUpperCase().replace(/[^A-Z]/g, "");
    const combined = `${first}${second}`.replace(/[^A-Z]/g, "");
    return (combined || "XX").slice(0, 2).padEnd(2, "X");
  }

  /** Next global employee wallet sequence (5 digits). */
  private async nextEmployeeWalletNumberSequence(): Promise<number> {
    const seqResult = await pool.query<{ next_val: number }>(
      "UPDATE employee_wallet_number_sequence SET next_val = next_val + 1 WHERE id = 1 RETURNING next_val"
    );
    return seqResult.rows[0]?.next_val ?? 1;
  }

  async assignCompanyAccountNumber(businessId: string, businessName: string): Promise<Business | undefined> {
    return withRetry("assignCompanyAccountNumber", async () => {
      const twoLetters = this.twoLettersFromName(businessName);
      const nextVal = await this.nextAccountNumberSequence();
      const numPart = String(nextVal).padStart(4, "0");
      const accountNumber = twoLetters + "B" + numPart;

      const [updated] = await db
        .update(businesses)
        .set({ accountNumber })
        .where(eq(businesses.id, businessId))
        .returning();
      return updated;
    });
  }

  async assignCustomerAccountNumber(userId: string, fullName: string): Promise<User | undefined> {
    return withRetry("assignCustomerAccountNumber", async () => {
      const twoLetters = this.twoLettersFromName(fullName);
      const nextVal = await this.nextAccountNumberSequence();
      const numPart = String(nextVal).padStart(4, "0");
      const accountNumber = twoLetters + "C" + numPart;

      const [updated] = await db
        .update(users)
        .set({ accountNumber })
        .where(eq(users.id, userId))
        .returning();
      return updated;
    });
  }

  async getBusinessesWithoutAccountNumber(): Promise<Business[]> {
    return db.select().from(businesses).where(sql`${businesses.accountNumber} IS NULL`).orderBy(asc(businesses.createdAt));
  }

  async getUsersWithoutAccountNumber(): Promise<User[]> {
    return db.select().from(users).where(sql`${users.accountNumber} IS NULL`).orderBy(asc(users.createdAt));
  }

  // ──────────── Approvals ────────────
  async createApproval(approval: InsertApproval): Promise<Approval> {
    const [result] = await db.insert(approvals).values({ ...approval, id: crypto.randomUUID() }).returning();
    return result;
  }

  async getApprovals(status?: string, makerIds?: string[]): Promise<Approval[]> {
    if (makerIds && makerIds.length === 0) return [];
    const makerFilter = makerIds != null && makerIds.length > 0 ? inArray(approvals.makerId, makerIds) : undefined;
    const baseWhere = status
      ? (makerFilter ? and(eq(approvals.status, status as any), makerFilter) : eq(approvals.status, status as any))
      : makerFilter;
    if (baseWhere) {
      return db.select().from(approvals).where(baseWhere).orderBy(desc(approvals.createdAt));
    }
    return db.select().from(approvals).orderBy(desc(approvals.createdAt));
  }

  async getApprovalById(id: string): Promise<Approval | undefined> {
    const [result] = await db.select().from(approvals).where(eq(approvals.id, id));
    return result;
  }

  async getLatestApprovalForEntity(entityType: string, entityId: string): Promise<Approval | undefined> {
    const [result] = await db
      .select()
      .from(approvals)
      .where(and(eq(approvals.entityType, entityType), eq(approvals.entityId, entityId)))
      .orderBy(desc(approvals.createdAt))
      .limit(1);
    return result;
  }

  async checkerAction(id: string, checkerId: string, checkerName: string, action: "approve" | "reject", note?: string): Promise<Approval | undefined> {
    const newStatus = action === "approve" ? "pending_approver" : "rejected";
    const [result] = await db.update(approvals).set({
      status: newStatus as any,
      checkerId,
      checkerName,
      checkerNote: note || null,
      checkerActionAt: new Date(),
    }).where(eq(approvals.id, id)).returning();
    return result;
  }

  async generateOtp(id: string, method: string): Promise<{ otp: string; approval: Approval } | undefined> {
    const otp = Math.floor(100000 + Math.random() * 900000).toString();
    const otpExpiresAt = new Date(Date.now() + 5 * 60 * 1000);
    const [result] = await db.update(approvals).set({
      otpCode: otp,
      otpExpiresAt,
      otpMethod: method,
    }).where(eq(approvals.id, id)).returning();
    if (!result) return undefined;
    return { otp, approval: result };
  }

  async verifyOtpAndApprove(id: string, code: string, approverId: string, approverName: string, note?: string): Promise<Approval | undefined> {
    const approval = await this.getApprovalById(id);
    if (!approval) return undefined;
    if (approval.otpCode !== code) return undefined;
    if (approval.otpExpiresAt && new Date() > approval.otpExpiresAt) return undefined;

    const [result] = await db.update(approvals).set({
      status: "approved" as any,
      approverId,
      approverName,
      approverNote: note || null,
      approverActionAt: new Date(),
      otpCode: null,
      otpExpiresAt: null,
    }).where(eq(approvals.id, id)).returning();
    return result;
  }

  async rejectApproval(id: string, rejectorId: string, rejectorName: string, note?: string): Promise<Approval | undefined> {
    const [result] = await db.update(approvals).set({
      status: "rejected" as any,
      approverId: rejectorId,
      approverName: rejectorName,
      approverNote: note || null,
      approverActionAt: new Date(),
    }).where(eq(approvals.id, id)).returning();
    return result;
  }

  // ──────────── Bulk Payments ────────────
  async createBatch(batch: InsertBulkPaymentBatch): Promise<BulkPaymentBatch> {
    const [result] = await db.insert(bulkPaymentBatches).values({ ...batch, id: crypto.randomUUID() }).returning();
    return result;
  }

  async getBatches(userId?: string): Promise<BulkPaymentBatch[]> {
    if (userId) {
      return db
        .select()
        .from(bulkPaymentBatches)
        .where(eq(bulkPaymentBatches.createdByUserId, userId))
        .orderBy(desc(bulkPaymentBatches.createdAt));
    }
    return db.select().from(bulkPaymentBatches).orderBy(desc(bulkPaymentBatches.createdAt));
  }

  async getBatchesByUserIds(
    userIds: string[],
    options?: { walletBusinessId?: string | null },
  ): Promise<BulkPaymentBatch[]> {
    const walletBusinessId = options?.walletBusinessId ?? null;
    if (!userIds.length && !walletBusinessId) return [];
    if (walletBusinessId) {
      const creatorMatch = userIds.length
        ? inArray(bulkPaymentBatches.createdByUserId, userIds)
        : sql`false`;
      return db
        .select()
        .from(bulkPaymentBatches)
        .where(
          or(creatorMatch, eq(bulkPaymentBatches.walletBusinessId, walletBusinessId)),
        )
        .orderBy(desc(bulkPaymentBatches.createdAt));
    }
    return db
      .select()
      .from(bulkPaymentBatches)
      .where(inArray(bulkPaymentBatches.createdByUserId, userIds))
      .orderBy(desc(bulkPaymentBatches.createdAt));
  }

  async getFirstSuccessfulBulkPayoutAt(actorUserIds: string[]): Promise<Date | null> {
    if (!actorUserIds.length) return null;
    const [row] = await db
      .select({
        firstAt: sql<Date | null>`MIN(COALESCE(${bulkPaymentItems.processedAt}, ${bulkPaymentBatches.createdAt}))`.as(
          "first_at",
        ),
      })
      .from(bulkPaymentItems)
      .innerJoin(bulkPaymentBatches, eq(bulkPaymentItems.batchId, bulkPaymentBatches.id))
      .innerJoin(users, eq(bulkPaymentBatches.createdByUserId, users.id))
      .leftJoin(businesses, eq(users.businessId, businesses.id))
      .where(
        and(
          inArray(bulkPaymentBatches.createdByUserId, actorUserIds),
          eq(bulkPaymentItems.status, "completed"),
          or(isNull(businesses.id), eq(businesses.isSandbox, false)),
        ),
      );
    const raw = row?.firstAt;
    if (raw == null) return null;
    return raw instanceof Date ? raw : new Date(raw);
  }

  async getBatch(id: string): Promise<BulkPaymentBatch | undefined> {
    const [result] = await db.select().from(bulkPaymentBatches).where(eq(bulkPaymentBatches.id, id));
    return result;
  }

  async updateBatchStatus(id: string, status: string): Promise<BulkPaymentBatch | undefined> {
    const [result] = await db.update(bulkPaymentBatches)
      .set({ status: status as any })
      .where(eq(bulkPaymentBatches.id, id))
      .returning();
    return result;
  }

  async createPaymentItem(item: InsertBulkPaymentItem): Promise<BulkPaymentItem> {
    const [result] = await db.insert(bulkPaymentItems).values({ ...item, id: crypto.randomUUID() }).returning();
    return result;
  }

  async getPaymentItemsByBatch(batchId: string): Promise<BulkPaymentItem[]> {
    return db.select().from(bulkPaymentItems).where(eq(bulkPaymentItems.batchId, batchId));
  }

  async getPaymentItemsByBatchIds(batchIds: string[]): Promise<BulkPaymentItem[]> {
    if (!batchIds.length) return [];
    return db.select().from(bulkPaymentItems).where(inArray(bulkPaymentItems.batchId, batchIds));
  }

  async getPaymentItemById(id: string): Promise<BulkPaymentItem | undefined> {
    const [result] = await db.select().from(bulkPaymentItems).where(eq(bulkPaymentItems.id, id));
    return result;
  }

  async updatePaymentItemStatus(id: string, status: "pending" | "processing" | "completed" | "failed" | "float_hold", failureReason?: string): Promise<BulkPaymentItem | undefined> {
    const updates: Record<string, any> = { status: status as any };
    if ((status === "failed" || status === "float_hold") && failureReason) {
      updates.failureReason = failureReason;
    }
    if (status === "pending") {
      updates.failureReason = null;
    }
    if (status === "completed" || status === "failed") {
      updates.processedAt = new Date();
    }
    const [result] = await db.update(bulkPaymentItems)
      .set(updates)
      .where(eq(bulkPaymentItems.id, id))
      .returning();
    return result;
  }

  async tryClaimBulkItemForB2cDispatch(itemId: string): Promise<{ claimed: boolean; row: BulkPaymentItem | undefined }> {
    const [fromPending] = await db
      .update(bulkPaymentItems)
      .set({ status: "processing" as any })
      .where(and(eq(bulkPaymentItems.id, itemId), eq(bulkPaymentItems.status, "pending" as any)))
      .returning();
    if (fromPending) return { claimed: true, row: fromPending };
    const [fromFloat] = await db
      .update(bulkPaymentItems)
      .set({ status: "processing" as any })
      .where(and(eq(bulkPaymentItems.id, itemId), eq(bulkPaymentItems.status, "float_hold" as any)))
      .returning();
    if (fromFloat) return { claimed: true, row: fromFloat };
    const row = await this.getPaymentItemById(itemId);
    return { claimed: false, row };
  }

  async tryClaimBulkItemForB2bDispatch(itemId: string): Promise<{ claimed: boolean; row: BulkPaymentItem | undefined }> {
    const [fromPending] = await db
      .update(bulkPaymentItems)
      .set({ status: "processing" as any })
      .where(and(eq(bulkPaymentItems.id, itemId), eq(bulkPaymentItems.status, "pending" as any)))
      .returning();
    if (fromPending) return { claimed: true, row: fromPending };
    const row = await this.getPaymentItemById(itemId);
    return { claimed: false, row };
  }

  async updatePaymentItemFromMpesaCallback(
    itemId: string,
    status: "completed" | "failed" | "float_hold",
    opts?: { failureReason?: string; mpesaTransactionId?: string; recipientNameFromMpesa?: string },
  ): Promise<BulkPaymentItem | undefined> {
    const updates: Record<string, any> = {
      status: status as any,
      // float_hold items are not finished — don't stamp processedAt
      ...(status !== "float_hold" && { processedAt: new Date() }),
      ...(opts?.failureReason !== undefined && { failureReason: opts.failureReason || null }),
      ...(opts?.mpesaTransactionId !== undefined && { mpesaTransactionId: opts.mpesaTransactionId || null }),
      ...(opts?.recipientNameFromMpesa !== undefined && { recipientNameFromMpesa: opts.recipientNameFromMpesa || null }),
    };
    const [result] = await db.update(bulkPaymentItems)
      .set(updates)
      .where(eq(bulkPaymentItems.id, itemId))
      .returning();
    return result;
  }

  async patchBulkPaymentItemRecipientNameFromMpesa(itemId: string, name: string): Promise<void> {
    const trimmed = name.trim();
    if (!trimmed) return;
    await db
      .update(bulkPaymentItems)
      .set({ recipientNameFromMpesa: trimmed })
      .where(eq(bulkPaymentItems.id, itemId));
  }

  async getPaymentItemsInFloatHold(): Promise<BulkPaymentItem[]> {
    return db.select().from(bulkPaymentItems).where(eq(bulkPaymentItems.status, "float_hold" as any));
  }

  async getPaymentItemsInFloatHoldOlderThan(hours: number): Promise<BulkPaymentItem[]> {
    const cutoff = new Date(Date.now() - hours * 60 * 60 * 1000);
    const rows = await db
      .select({ item: bulkPaymentItems })
      .from(bulkPaymentItems)
      .innerJoin(bulkPaymentBatches, eq(bulkPaymentItems.batchId, bulkPaymentBatches.id))
      .where(
        and(
          eq(bulkPaymentItems.status, "float_hold" as any),
          lt(bulkPaymentBatches.createdAt, cutoff),
        )
      );
    return rows.map((r) => r.item);
  }

  /**
   * Read the most-recent account_balance_result from M-Pesa audit logs and parse
   * the balance value. Uses the same format as the admin/wallets page.
   * Returns null if no balance result has ever been recorded.
   */
  async getLatestAccountBalance(): Promise<{ totalKes: number; rawValue: string; recordedAt: Date } | null> {
    const [log] = await db.select()
      .from(mpesaAuditLogs)
      .where(eq(mpesaAuditLogs.type, "account_balance_result"))
      .orderBy(desc(mpesaAuditLogs.createdAt))
      .limit(1);
    if (!log) return null;

    try {
      const parsed = JSON.parse(log.payload) as Record<string, any>;
      const params: Array<{ Key: string; Value: unknown }> =
        parsed?.Result?.ResultParameters?.ResultParameter ?? [];
      const entry = params.find((p) => p.Key === "AccountBalance");
      if (!entry?.Value) return null;

      const raw = String(entry.Value);
      // Format: "AccountName|Currency|Amount&AccountName|Currency|Amount"
      let total = 0;
      for (const segment of raw.split("&")) {
        const parts = segment.split("|");
        const amtStr = parts[2] ?? parts[1]; // Name|KES|Amount or Name[KES]Amount fallback
        const amt = parseFloat(amtStr ?? "");
        if (!Number.isNaN(amt)) total += amt;
      }
      return { totalKes: total, rawValue: raw, recordedAt: log.createdAt };
    } catch {
      return null;
    }
  }

  async setItemReversalPending(itemId: string, conversationId: string): Promise<BulkPaymentItem | undefined> {
    const [result] = await db.update(bulkPaymentItems)
      .set({ reversalStatus: "pending", reversalConversationId: conversationId, reversalInitiatedAt: new Date(), reversalResultDesc: null })
      .where(eq(bulkPaymentItems.id, itemId))
      .returning();
    return result;
  }

  async updateItemReversalStatus(conversationId: string, status: "success" | "failed", resultDesc?: string): Promise<BulkPaymentItem | undefined> {
    const [result] = await db.update(bulkPaymentItems)
      .set({ reversalStatus: status, ...(resultDesc !== undefined && { reversalResultDesc: resultDesc }) })
      .where(eq(bulkPaymentItems.reversalConversationId, conversationId))
      .returning();
    return result;
  }

  async getExpenseJournalPage(
    actorIds: string[],
    opts: { page: number; limit: number; status?: string; category?: string; search?: string; dateFrom?: Date; dateTo?: Date },
  ): Promise<{ items: ExpenseJournalRow[]; total: number; totalAmount: string; byCategory: [string, number][] }> {
    if (!actorIds.length) return { items: [], total: 0, totalAmount: "0", byCategory: [] };

    const offset = (opts.page - 1) * opts.limit;

    // Inline keyword categorizer matching expenseCategories.ts
    function deriveCategory(description: string): string {
      const d = description.toLowerCase();
      if (d.includes("salary") || d.includes("payroll") || d.includes("wage")) return "Salaries & Wages";
      if (d.includes("rent") || d.includes("lease") || d.includes("office space")) return "Rent & Utilities";
      if (d.includes("supplier") || d.includes("vendor") || d.includes("purchase") || d.includes("inventory") || d.includes("stock")) return "Suppliers & Inventory";
      if (d.includes("transport") || d.includes("fuel") || d.includes("logistics") || d.includes("delivery")) return "Transport & Logistics";
      if (d.includes("marketing") || d.includes("advert") || d.includes("promotion")) return "Marketing";
      if (d.includes("tax") || d.includes("kra") || d.includes("vat") || d.includes("levy") || d.includes("compliance")) return "Tax & Compliance";
      if (d.includes("loan") || d.includes("repayment") || d.includes("interest") || d.includes("advance")) return "Loan Repayments";
      if (d.includes("utility") || d.includes("water") || d.includes("electric") || d.includes("internet") || d.includes("airtime")) return "Utilities & Telecom";
      return "Other Expenses";
    }

    // Build WHERE conditions (run twice: once for page, once for aggregates)
    const buildConditions = () => {
      const conds: SQL[] = [inArray(bulkPaymentBatches.createdByUserId, actorIds)];
      if (opts.status && opts.status !== "all") {
        const st = opts.status === "successful" ? "completed" : opts.status;
        conds.push(eq(bulkPaymentItems.status, st as any));
      }
      const dateCol = sql`COALESCE(${bulkPaymentItems.processedAt}, ${bulkPaymentBatches.createdAt})`;
      if (opts.dateFrom) conds.push(gte(dateCol, opts.dateFrom));
      if (opts.dateTo)   conds.push(lt(dateCol, opts.dateTo));
      if (opts.search) {
        const t = `%${opts.search}%`;
        conds.push(or(ilike(bulkPaymentItems.reference, t), ilike(bulkPaymentItems.recipient, t))!);
      }
      if (opts.category && opts.category !== "all") {
        if (opts.category === "Other Expenses") {
          conds.push(or(eq(bulkPaymentItems.expenseCategory, "Other Expenses"), sql`${bulkPaymentItems.expenseCategory} IS NULL`)!);
        } else {
          conds.push(eq(bulkPaymentItems.expenseCategory, opts.category));
        }
      }
      return and(...conds)!;
    };

    const [rows, [countRow], aggRows] = await Promise.all([
      db.select({
        itemId:            bulkPaymentItems.id,
        batchId:           bulkPaymentBatches.id,
        recipient:         bulkPaymentItems.recipient,
        amount:            bulkPaymentItems.amount,
        fee:               bulkPaymentItems.fee,
        reference:         bulkPaymentItems.reference,
        status:            bulkPaymentItems.status,
        expenseCategory:   bulkPaymentItems.expenseCategory,
        processedAt:       bulkPaymentItems.processedAt,
        batchCreatedAt:    bulkPaymentBatches.createdAt,
        batchType:         bulkPaymentBatches.batchType,
        paymentType:       bulkPaymentBatches.paymentType,
        createdByName:     bulkPaymentBatches.createdByName,
        mpesaTransactionId: bulkPaymentItems.mpesaTransactionId,
        systemRef:         bulkPaymentItems.systemRef,
      })
      .from(bulkPaymentItems)
      .innerJoin(bulkPaymentBatches, eq(bulkPaymentItems.batchId, bulkPaymentBatches.id))
      .where(buildConditions())
      .orderBy(desc(sql`COALESCE(${bulkPaymentItems.processedAt}, ${bulkPaymentBatches.createdAt})`))
      .limit(opts.limit)
      .offset(offset),

      db.select({ count: sql<number>`count(*)::int` })
        .from(bulkPaymentItems)
        .innerJoin(bulkPaymentBatches, eq(bulkPaymentItems.batchId, bulkPaymentBatches.id))
        .where(buildConditions()),

      db.select({
        category:    sql<string>`COALESCE(${bulkPaymentItems.expenseCategory}, 'Other Expenses')`,
        catAmount:   sql<string>`SUM(${bulkPaymentItems.amount})`,
      })
      .from(bulkPaymentItems)
      .innerJoin(bulkPaymentBatches, eq(bulkPaymentItems.batchId, bulkPaymentBatches.id))
      .where(buildConditions())
      .groupBy(sql`COALESCE(${bulkPaymentItems.expenseCategory}, 'Other Expenses')`),
    ]);

    const totalAmount = aggRows.reduce((s, r) => s + parseFloat(r.catAmount ?? "0"), 0).toFixed(2);
    const byCategory = aggRows
      .map(r => [r.category, parseFloat(r.catAmount ?? "0")] as [string, number])
      .sort((a, b) => b[1] - a[1]);

    const items: ExpenseJournalRow[] = rows.map(r => ({
      ...r,
      expenseCategory: r.expenseCategory
        ?? deriveCategory(`${String(r.reference ?? "")} Disbursement to ${r.recipient}`),
    }));

    return { items, total: countRow?.count ?? 0, totalAmount, byCategory };
  }

  async updatePaymentItemCategory(itemId: string, category: string): Promise<void> {
    await db.update(bulkPaymentItems)
      .set({ expenseCategory: category })
      .where(eq(bulkPaymentItems.id, itemId));
  }

  async finalizeBatchStatus(batchId: string): Promise<BulkPaymentBatch | undefined> {
    const items = await this.getPaymentItemsByBatch(batchId);
    if (items.length === 0) return this.getBatch(batchId);

    const completed = items.filter(i => i.status === "completed").length;
    const failed = items.filter(i => i.status === "failed").length;
    const pending = items.filter(i => i.status === "pending").length;
    const processing = items.filter(i => i.status === "processing").length;

    let batchStatus: string;
    if (pending > 0 || processing > 0) {
      batchStatus = "processing";
    } else if (failed === 0) {
      batchStatus = "completed";
    } else if (completed === 0) {
      batchStatus = "failed";
    } else {
      batchStatus = "completed";
    }

    const [result] = await db.update(bulkPaymentBatches)
      .set({
        status: batchStatus as any,
        completedCount: completed,
        failedCount: failed,
        processedAt: (pending === 0 && processing === 0) ? new Date() : null,
      })
      .where(eq(bulkPaymentBatches.id, batchId))
      .returning();
    return result;
  }

  private mapB2cDispatchQueueRow(r: Record<string, unknown>): B2cDispatchQueueRow {
    return {
      id: String(r.id),
      itemId: String(r.item_id),
      batchId: String(r.batch_id),
      businessId: r.business_id != null ? String(r.business_id) : null,
      status: String(r.status),
      leaseOwner: r.lease_owner != null ? String(r.lease_owner) : null,
      leaseUntil: r.lease_until != null ? new Date(String(r.lease_until)) : null,
      enqueuedByUserId: r.enqueued_by_user_id != null ? String(r.enqueued_by_user_id) : null,
      enqueuedByName: r.enqueued_by_name != null ? String(r.enqueued_by_name) : null,
      createdAt: new Date(String(r.created_at)),
      attemptCount: Number(r.attempt_count ?? 0),
    };
  }

  async enqueueB2cDispatchJobs(input: {
    batchId: string;
    itemIds: string[];
    businessId: string | null;
    userId?: string | null;
    userName?: string | null;
  }): Promise<number> {
    if (input.itemIds.length === 0) return 0;
    const result = await pool.query(
      `INSERT INTO b2c_dispatch_queue (item_id, batch_id, business_id, enqueued_by_user_id, enqueued_by_name)
       SELECT i.id, $2::varchar, $3::varchar, $4::varchar, $5::text
       FROM unnest($1::varchar[]) AS x(item_id)
       INNER JOIN bulk_payment_items i ON i.id = x.item_id AND i.batch_id = $2::varchar
       ON CONFLICT (item_id) WHERE (status IN ('pending', 'leased')) DO NOTHING`,
      [
        input.itemIds,
        input.batchId,
        input.businessId,
        input.userId ?? null,
        input.userName ?? null,
      ],
    );
    return result.rowCount ?? 0;
  }

  async countB2cDispatchActiveForBatch(batchId: string): Promise<number> {
    const { rows } = await pool.query<{ n: string }>(
      `SELECT count(*)::text AS n FROM b2c_dispatch_queue
       WHERE batch_id = $1 AND status IN ('pending', 'leased')`,
      [batchId],
    );
    return Number.parseInt(rows[0]?.n ?? "0", 10) || 0;
  }

  async completeB2cDispatchJob(id: string, leaseOwner: string): Promise<boolean> {
    const r = await pool.query(`DELETE FROM b2c_dispatch_queue WHERE id = $1 AND lease_owner = $2`, [
      id,
      leaseOwner,
    ]);
    return (r.rowCount ?? 0) > 0;
  }

  async requeueB2cDispatchJob(id: string, leaseOwner: string): Promise<boolean> {
    const r = await pool.query(
      `UPDATE b2c_dispatch_queue
       SET status = 'pending', lease_owner = NULL, lease_until = NULL
       WHERE id = $1 AND lease_owner = $2`,
      [id, leaseOwner],
    );
    return (r.rowCount ?? 0) > 0;
  }

  async releaseExpiredB2cDispatchLeasesTx(client: PoolClient): Promise<void> {
    await client.query(
      `UPDATE b2c_dispatch_queue
       SET status = 'pending', lease_owner = NULL, lease_until = NULL
       WHERE status = 'leased' AND lease_until IS NOT NULL AND lease_until < NOW()`,
    );
  }

  async claimFairB2cDispatchJobsTx(
    client: PoolClient,
    limit: number,
    leaseOwner: string,
    leaseSeconds: number,
  ): Promise<B2cDispatchQueueRow[]> {
    const lim = Math.max(1, Math.min(500, Math.floor(limit)));
    const secs = Math.max(30, Math.min(3600, Math.floor(leaseSeconds)));
    const { rows } = await client.query<Record<string, unknown>>(
      `WITH ranked AS (
         SELECT q.id, q.created_at,
           ROW_NUMBER() OVER (PARTITION BY COALESCE(q.business_id, '') ORDER BY q.created_at ASC, q.id ASC) AS biz_rn
         FROM b2c_dispatch_queue q
         WHERE q.status = 'pending'
       ),
       ordered AS (
         SELECT ranked.id, ranked.created_at,
           ROW_NUMBER() OVER (ORDER BY ranked.biz_rn ASC, ranked.created_at ASC, ranked.id ASC) AS fair_rn
         FROM ranked
       ),
       picked AS (
         SELECT q.id
         FROM ordered o
         INNER JOIN b2c_dispatch_queue q ON q.id = o.id AND q.status = 'pending'
         WHERE o.fair_rn <= $1
         ORDER BY o.fair_rn
         FOR UPDATE OF q SKIP LOCKED
       )
       UPDATE b2c_dispatch_queue q
       SET status = 'leased',
           lease_owner = $2,
           lease_until = NOW() + ($3::double precision * INTERVAL '1 second'),
           attempt_count = q.attempt_count + 1
       FROM picked p
       WHERE q.id = p.id
       RETURNING q.*`,
      [lim, leaseOwner, secs],
    );
    return rows.map((r) => this.mapB2cDispatchQueueRow(r));
  }

  async getDueScheduledB2CBatches(limit = 10): Promise<BulkPaymentBatch[]> {
    return db
      .select()
      .from(bulkPaymentBatches)
      .where(
        and(
          eq(bulkPaymentBatches.batchType, "scheduled"),
          eq(bulkPaymentBatches.status, "pending"),
          sql`${bulkPaymentBatches.scheduledFor} IS NOT NULL`,
          sql`${bulkPaymentBatches.scheduledFor} <= NOW()`,
        ),
      )
      .orderBy(bulkPaymentBatches.scheduledFor)
      .limit(limit);
  }

  async getScheduledBatches(userId: string, limit = 8): Promise<BulkPaymentBatch[]> {
    return db
      .select()
      .from(bulkPaymentBatches)
      .where(
        and(
          eq(bulkPaymentBatches.createdByUserId, userId),
          eq(bulkPaymentBatches.batchType, "scheduled"),
          sql`${bulkPaymentBatches.status} IN ('pending', 'processing')`,
        ),
      )
      .orderBy(bulkPaymentBatches.scheduledFor)
      .limit(limit);
  }

  async getScheduledBatchesByBusiness(
    businessId: string,
    opts: { page: number; limit: number; status?: string },
  ): Promise<{ batches: BulkPaymentBatch[]; total: number }> {
    const { page, limit, status } = opts;
    const offset = (page - 1) * limit;

    // Match batches created by anyone associated with this business:
    //   1. The business owner (businesses.user_id)
    //   2. Direct team members (users.business_id = businessId)
    //   3. Legacy team members (no business_id set, created_by = owner)
    const creatorInBusiness = sql`(
      ${bulkPaymentBatches.createdByUserId} = (SELECT user_id FROM businesses WHERE id = ${businessId})
      OR ${bulkPaymentBatches.createdByUserId} IN (SELECT id FROM users WHERE business_id = ${businessId})
      OR ${bulkPaymentBatches.createdByUserId} IN (
        SELECT id FROM users
        WHERE created_by = (SELECT user_id FROM businesses WHERE id = ${businessId})
          AND business_id IS NULL
      )
    )`;

    const conditions: SQL[] = [
      creatorInBusiness,
      eq(bulkPaymentBatches.batchType, "scheduled"),
    ];
    if (status === "pending_active") {
      // Widget: show batches that haven't run yet
      conditions.push(sql`${bulkPaymentBatches.status} IN ('pending', 'processing')`);
    } else if (status && status !== "all") {
      conditions.push(sql`${bulkPaymentBatches.status} = ${status}`);
    }
    const where = and(...conditions);

    const [{ total }] = await db
      .select({ total: sql<number>`count(*)::int` })
      .from(bulkPaymentBatches)
      .where(where);

    const batches = await db
      .select()
      .from(bulkPaymentBatches)
      .where(where)
      .orderBy(desc(bulkPaymentBatches.scheduledFor))
      .limit(limit)
      .offset(offset);

    return { batches, total };
  }

  async getAllScheduledBatchesPaginated(opts: { page: number; limit: number; status?: string; actorIds?: string[] }): Promise<{ batches: BulkPaymentBatch[]; total: number }> {
    const { page, limit, status, actorIds } = opts;
    const offset = (page - 1) * limit;
    // Core filter: batch_type = 'scheduled'
    const conditions: SQL[] = [eq(bulkPaymentBatches.batchType, "scheduled")];
    // Scope to company users when not super_admin
    if (actorIds && actorIds.length > 0) {
      conditions.push(sql`${bulkPaymentBatches.createdByUserId} = ANY(ARRAY[${sql.join(actorIds.map(id => sql`${id}`), sql`, `)}]::text[])`);
    }
    if (status === "pending_active") {
      conditions.push(sql`${bulkPaymentBatches.status} IN ('pending', 'processing')`);
    } else if (status && status !== "all") {
      conditions.push(sql`${bulkPaymentBatches.status} = ${status}`);
    }
    const where = and(...conditions);
    const [{ total }] = await db.select({ total: sql<number>`count(*)::int` }).from(bulkPaymentBatches).where(where);
    const batches = await db.select().from(bulkPaymentBatches).where(where).orderBy(desc(bulkPaymentBatches.scheduledFor)).limit(limit).offset(offset);
    return { batches, total };
  }

  async holdBatch(batchId: string, heldBy: string, heldByName: string, reason: string): Promise<BulkPaymentBatch | undefined> {
    const [batch] = await db.select().from(bulkPaymentBatches).where(eq(bulkPaymentBatches.id, batchId));
    if (!batch || batch.status !== "pending") return undefined;
    const [updated] = await db.update(bulkPaymentBatches)
      .set({ status: "held", heldReason: reason, heldBy, heldByName, heldAt: new Date(), releasedAt: null })
      .where(eq(bulkPaymentBatches.id, batchId))
      .returning();
    return updated;
  }

  async releaseBatch(batchId: string): Promise<BulkPaymentBatch | undefined> {
    const [updated] = await db.update(bulkPaymentBatches)
      .set({ status: "pending", releasedAt: new Date() })
      .where(and(eq(bulkPaymentBatches.id, batchId), eq(bulkPaymentBatches.status, "held")))
      .returning();
    return updated;
  }

  async getHeldBatchesByBusiness(businessId: string, opts: { page: number; limit: number }): Promise<{ batches: BulkPaymentBatch[]; total: number }> {
    const { page, limit } = opts;

    const creatorInBusiness = sql`(
      ${bulkPaymentBatches.createdByUserId} = (SELECT user_id FROM businesses WHERE id = ${businessId})
      OR ${bulkPaymentBatches.createdByUserId} IN (SELECT id FROM users WHERE business_id = ${businessId})
      OR ${bulkPaymentBatches.createdByUserId} IN (
        SELECT id FROM users
        WHERE created_by = (SELECT user_id FROM businesses WHERE id = ${businessId})
          AND business_id IS NULL
      )
    )`;

    const conditions: SQL[] = [
      eq(bulkPaymentBatches.status, "held"),
      creatorInBusiness,
    ];
    const where = and(...conditions);

    const [{ total }] = await db
      .select({ total: sql<number>`count(*)::int` })
      .from(bulkPaymentBatches)
      .where(where);

    const batches = await db
      .select()
      .from(bulkPaymentBatches)
      .where(where)
      .orderBy(desc(bulkPaymentBatches.heldAt))
      .limit(limit)
      .offset((page - 1) * limit);

    return { batches, total };
  }

  // ── Stale payment reconciliation ────────────────────────────────────────────

  async getStaleProcessingItems(olderThanMinutes = 30, limit = 200): Promise<BulkPaymentItem[]> {
    const cutoff = new Date(Date.now() - olderThanMinutes * 60 * 1000);
    // Join to batches; exclude sandbox businesses — sandbox items never receive real M-Pesa callbacks
    const rows = await db
      .select({ item: bulkPaymentItems })
      .from(bulkPaymentItems)
      .innerJoin(bulkPaymentBatches, eq(bulkPaymentItems.batchId, bulkPaymentBatches.id))
      .where(and(
        eq(bulkPaymentItems.status, "processing"),
        lt(bulkPaymentBatches.createdAt, cutoff),
        // Exclude items belonging to sandbox businesses
        sql`NOT EXISTS (
          SELECT 1 FROM businesses b
          WHERE b.is_sandbox = true
            AND (
              b.user_id = ${bulkPaymentBatches.createdByUserId}
              OR b.id IN (
                SELECT business_id FROM users WHERE id = ${bulkPaymentBatches.createdByUserId} AND business_id IS NOT NULL
              )
            )
        )`,
      ))
      .orderBy(asc(bulkPaymentBatches.createdAt))
      .limit(limit);
    return rows.map(r => r.item);
  }

  /** Pending items that were never sent: batch created long ago (or scheduled batch already due). Release reserved funds by force-failing. Applies to single and bulk disbursements (both use bulk_payment_items). */
  async getStalePendingItems(olderThanMinutes = 90, limit = 200): Promise<BulkPaymentItem[]> {
    const cutoff = new Date(Date.now() - olderThanMinutes * 60 * 1000);
    const rows = await db
      .select({ item: bulkPaymentItems })
      .from(bulkPaymentItems)
      .innerJoin(bulkPaymentBatches, eq(bulkPaymentItems.batchId, bulkPaymentBatches.id))
      .where(and(
        eq(bulkPaymentItems.status, "pending"),
        lt(bulkPaymentBatches.createdAt, cutoff),
        // Scheduled batches: only if already due (scheduled_for in the past or null)
        or(
          sql`${bulkPaymentBatches.batchType} = 'direct'`,
          sql`${bulkPaymentBatches.scheduledFor} IS NULL`,
          sql`${bulkPaymentBatches.scheduledFor} <= NOW()`,
        ),
        sql`NOT EXISTS (
          SELECT 1 FROM businesses b
          WHERE b.is_sandbox = true
            AND (
              b.user_id = ${bulkPaymentBatches.createdByUserId}
              OR b.id IN (
                SELECT business_id FROM users WHERE id = ${bulkPaymentBatches.createdByUserId} AND business_id IS NOT NULL
              )
            )
        )`,
      ))
      .orderBy(asc(bulkPaymentBatches.createdAt))
      .limit(limit);
    return rows.map(r => r.item);
  }

  async incrementItemQueryAttempt(itemId: string): Promise<void> {
    await db.update(bulkPaymentItems)
      .set({
        queryAttempts: sql`${bulkPaymentItems.queryAttempts} + 1`,
        lastQueriedAt: new Date(),
      })
      .where(eq(bulkPaymentItems.id, itemId));
  }

  async forceResolveItem(itemId: string, status: "completed" | "failed", reason?: string): Promise<BulkPaymentItem | undefined> {
    const [item] = await db.update(bulkPaymentItems)
      .set({ status, failureReason: reason || null, processedAt: new Date() })
      .where(eq(bulkPaymentItems.id, itemId))
      .returning();
    if (item) {
      await this.finalizeBatchStatus(item.batchId);
      void import("./services/b2cQueue")
        .then((m) =>
          m.notifyScheduledB2cWhenReady(item.batchId).catch((e: unknown) =>
            console.warn("[storage] notifyScheduledB2cWhenReady:", e instanceof Error ? e.message : e),
          ),
        )
        .catch(() => {});
    }
    return item;
  }

  async getStaleItemsForReconciliation(opts: {
    page: number;
    limit: number;
    businessId?: string;
    status?: string;
    search?: string;
  }): Promise<{ items: any[]; total: number }> {
    const { page, limit, businessId, status, search } = opts;

    // Status-based item condition
    let itemStatusCond: SQL | undefined;
    if (status === "pending") {
      itemStatusCond = and(
        eq(bulkPaymentItems.status, "processing"),
        sql`${bulkPaymentItems.queryAttempts} < 3`,
      );
    } else if (status === "escalated") {
      itemStatusCond = and(
        eq(bulkPaymentItems.status, "processing"),
        sql`${bulkPaymentItems.queryAttempts} >= 3`,
      );
    } else if (status === "resolved") {
      itemStatusCond = and(
        sql`${bulkPaymentItems.status} IN ('completed', 'failed')`,
        sql`${bulkPaymentItems.queryAttempts} > 0`,
      );
    } else {
      // All stale: processing (any attempts) OR resolved-after-stale
      itemStatusCond = or(
        eq(bulkPaymentItems.status, "processing"),
        and(
          sql`${bulkPaymentItems.status} IN ('completed', 'failed')`,
          sql`${bulkPaymentItems.queryAttempts} > 0`,
        ),
      );
    }

    // Business scope subquery (same as getHeldBatchesByBusiness)
    let batchCond: SQL | undefined;
    if (businessId) {
      batchCond = sql`(
        ${bulkPaymentBatches.createdByUserId} = (SELECT user_id FROM businesses WHERE id = ${businessId})
        OR ${bulkPaymentBatches.createdByUserId} IN (SELECT id FROM users WHERE business_id = ${businessId})
        OR ${bulkPaymentBatches.createdByUserId} IN (
          SELECT id FROM users
          WHERE created_by = (SELECT user_id FROM businesses WHERE id = ${businessId})
            AND business_id IS NULL
        )
      )`;
    }

    // Search condition
    let searchCond: SQL | undefined;
    if (search?.trim()) {
      const term = `%${search.trim()}%`;
      searchCond = or(
        like(bulkPaymentItems.recipient, term),
        like(bulkPaymentItems.reference, term),
        like(bulkPaymentBatches.createdByName, term),
      );
    }

    const conditions = [itemStatusCond, batchCond, searchCond].filter(Boolean) as SQL[];
    const where = conditions.length > 0 ? and(...conditions) : undefined;

    const [{ total }] = await db
      .select({ total: sql<number>`count(*)::int` })
      .from(bulkPaymentItems)
      .innerJoin(bulkPaymentBatches, eq(bulkPaymentItems.batchId, bulkPaymentBatches.id))
      .where(where);

    const rows = await db
      .select({
        itemId: bulkPaymentItems.id,
        recipient: bulkPaymentItems.recipient,
        amount: bulkPaymentItems.amount,
        fee: bulkPaymentItems.fee,
        reference: bulkPaymentItems.reference,
        itemStatus: bulkPaymentItems.status,
        failureReason: bulkPaymentItems.failureReason,
        queryAttempts: bulkPaymentItems.queryAttempts,
        lastQueriedAt: bulkPaymentItems.lastQueriedAt,
        processedAt: bulkPaymentItems.processedAt,
        batchId: bulkPaymentBatches.id,
        paymentType: bulkPaymentBatches.paymentType,
        createdByName: bulkPaymentBatches.createdByName,
        batchCreatedAt: bulkPaymentBatches.createdAt,
        businessName: sql<string | null>`(
          SELECT b.business_name FROM businesses b
          WHERE b.user_id = ${bulkPaymentBatches.createdByUserId}
          LIMIT 1
        )`,
      })
      .from(bulkPaymentItems)
      .innerJoin(bulkPaymentBatches, eq(bulkPaymentItems.batchId, bulkPaymentBatches.id))
      .where(where)
      .orderBy(desc(bulkPaymentBatches.createdAt))
      .limit(limit)
      .offset((page - 1) * limit);

    return { items: rows, total };
  }

  async searchPaymentItemsForResolution(q: string): Promise<{ items: any[] }> {
    const term = `%${q.trim()}%`;
    const rows = await db
      .select({
        itemId: bulkPaymentItems.id,
        recipient: bulkPaymentItems.recipient,
        amount: bulkPaymentItems.amount,
        fee: bulkPaymentItems.fee,
        reference: bulkPaymentItems.reference,
        systemRef: bulkPaymentItems.systemRef,
        itemStatus: bulkPaymentItems.status,
        failureReason: bulkPaymentItems.failureReason,
        mpesaTransactionId: bulkPaymentItems.mpesaTransactionId,
        recipientNameFromMpesa: bulkPaymentItems.recipientNameFromMpesa,
        queryAttempts: bulkPaymentItems.queryAttempts,
        processedAt: bulkPaymentItems.processedAt,
        batchId: bulkPaymentBatches.id,
        paymentType: bulkPaymentBatches.paymentType,
        createdByName: bulkPaymentBatches.createdByName,
        batchCreatedAt: bulkPaymentBatches.createdAt,
        businessName: sql<string | null>`(
          SELECT b.business_name FROM businesses b
          WHERE b.user_id = ${bulkPaymentBatches.createdByUserId}
          LIMIT 1
        )`,
      })
      .from(bulkPaymentItems)
      .innerJoin(bulkPaymentBatches, eq(bulkPaymentItems.batchId, bulkPaymentBatches.id))
      .where(
        or(
          ilike(bulkPaymentItems.recipient, term),
          ilike(bulkPaymentItems.reference, term),
          ilike(bulkPaymentItems.systemRef, term),
          ilike(bulkPaymentItems.mpesaTransactionId, term),
          ilike(bulkPaymentItems.recipientNameFromMpesa, term),
          sql`${bulkPaymentItems.id}::text ILIKE ${term}`,
          sql`${bulkPaymentBatches.id}::text ILIKE ${term}`,
          sql`${bulkPaymentBatches.createdByName}::text ILIKE ${term}`,
        ),
      )
      .orderBy(desc(bulkPaymentBatches.createdAt))
      .limit(50);
    return { items: rows };
  }

  async getMpesaRecipientNameByPhone(businessId: string, normalizedPhone254: string): Promise<string | null> {
    const tenDigit = normalizedPhone254.length === 12 && normalizedPhone254.startsWith("254")
      ? "0" + normalizedPhone254.slice(3)
      : null;
    const batchCond = sql`(
      ${bulkPaymentBatches.createdByUserId} = (SELECT user_id FROM businesses WHERE id = ${businessId})
      OR ${bulkPaymentBatches.createdByUserId} IN (SELECT id FROM users WHERE business_id = ${businessId})
      OR ${bulkPaymentBatches.createdByUserId} IN (
        SELECT id FROM users
        WHERE created_by = (SELECT user_id FROM businesses WHERE id = ${businessId})
          AND business_id IS NULL
      )
    )`;
    const recipientMatch = tenDigit
      ? or(
          eq(bulkPaymentItems.recipient, normalizedPhone254),
          eq(bulkPaymentItems.recipient, tenDigit),
        )
      : eq(bulkPaymentItems.recipient, normalizedPhone254);
    const [row] = await db
      .select({ name: bulkPaymentItems.recipientNameFromMpesa })
      .from(bulkPaymentItems)
      .innerJoin(bulkPaymentBatches, eq(bulkPaymentItems.batchId, bulkPaymentBatches.id))
      .where(
        and(
          batchCond,
          eq(bulkPaymentBatches.paymentType, "mobile_money"),
          eq(bulkPaymentItems.status, "completed"),
          recipientMatch,
          sql`${bulkPaymentItems.recipientNameFromMpesa} IS NOT NULL AND ${bulkPaymentItems.recipientNameFromMpesa} != ''`,
          sql`${bulkPaymentItems.processedAt} IS NOT NULL`,
        ),
      )
      .orderBy(desc(bulkPaymentItems.processedAt))
      .limit(1);
    const raw = row?.name?.trim?.() ? String(row.name).trim() : "";
    if (!raw) return null;
    // Backward compatibility: older rows may have "2547... - NAME" stored.
    if (raw.includes(" - ")) return raw.split(" - ").slice(1).join(" - ").trim() || null;
    return raw;
  }

  async getTillRecipientNameByTill(businessId: string, tillNumber: string): Promise<string | null> {
    const normalized = String(tillNumber ?? "").replace(/\D/g, "").slice(0, 12);
    if (!normalized || normalized.length < 5) return null;
    const batchCond = sql`(
      ${bulkPaymentBatches.createdByUserId} = (SELECT user_id FROM businesses WHERE id = ${businessId})
      OR ${bulkPaymentBatches.createdByUserId} IN (SELECT id FROM users WHERE business_id = ${businessId})
      OR ${bulkPaymentBatches.createdByUserId} IN (
        SELECT id FROM users
        WHERE created_by = (SELECT user_id FROM businesses WHERE id = ${businessId})
          AND business_id IS NULL
      )
    )`;
    const tillMatch = or(
      eq(bulkPaymentItems.recipient, normalized),
      eq(bulkPaymentItems.accountNumber, normalized),
    );
    // Only names from a finished B2B leg (we have an M-Pesa txn id). Never use optional store/CSV labels here —
    // those are not Hakikisha-verified and were conflated with M-Pesa names.
    const rows = await db
      .select({ name: bulkPaymentItems.recipientNameFromMpesa, reference: bulkPaymentItems.reference })
      .from(bulkPaymentItems)
      .innerJoin(bulkPaymentBatches, eq(bulkPaymentItems.batchId, bulkPaymentBatches.id))
      .where(
        and(
          batchCond,
          eq(bulkPaymentBatches.paymentType, "buy_goods"),
          eq(bulkPaymentItems.status, "completed"),
          tillMatch,
          sql`${bulkPaymentItems.mpesaTransactionId} IS NOT NULL AND trim(${bulkPaymentItems.mpesaTransactionId}) != ''`,
        ),
      )
      .orderBy(desc(bulkPaymentItems.processedAt))
      .limit(20);

    for (const row of rows) {
      const fromMpesa = row?.name?.trim?.() ? String(row.name).trim() : "";
      if (fromMpesa) {
        if (fromMpesa.includes(" - ")) {
          const rest = fromMpesa.split(" - ").slice(1).join(" - ").trim();
          if (rest) return rest;
        } else return fromMpesa;
      }
      const fromRef = storeNameFromPaymentReference(row?.reference ?? null, normalized);
      if (fromRef) return fromRef;
    }
    return null;
  }

  async getQuickPayBuyGoodsTillLabel(userId: string, tillNumber: string): Promise<string | null> {
    const norm = String(tillNumber ?? "").replace(/\D/g, "").slice(0, 12);
    if (!norm || norm.length < 5) return null;
    const [row] = await db
      .select({ name: quickPayRecipients.name })
      .from(quickPayRecipients)
      .where(
        and(
          eq(quickPayRecipients.userId, userId),
          eq(quickPayRecipients.paymentType, "buy_goods"),
          sql`regexp_replace(coalesce(${quickPayRecipients.accountNumber}, ''), '[^0-9]', '', 'g') = ${norm}`,
        ),
      )
      .orderBy(desc(quickPayRecipients.createdAt))
      .limit(1);
    const n = row?.name?.trim();
    return n || null;
  }

  async hasCompletedBuyGoodsPayoutToTill(businessId: string, tillNumber: string): Promise<boolean> {
    const normalized = String(tillNumber ?? "").replace(/\D/g, "").slice(0, 12);
    if (!normalized || normalized.length < 5) return false;
    const batchCond = sql`(
      ${bulkPaymentBatches.createdByUserId} = (SELECT user_id FROM businesses WHERE id = ${businessId})
      OR ${bulkPaymentBatches.createdByUserId} IN (SELECT id FROM users WHERE business_id = ${businessId})
      OR ${bulkPaymentBatches.createdByUserId} IN (
        SELECT id FROM users
        WHERE created_by = (SELECT user_id FROM businesses WHERE id = ${businessId})
          AND business_id IS NULL
      )
    )`;
    const tillMatch = or(
      eq(bulkPaymentItems.recipient, normalized),
      eq(bulkPaymentItems.accountNumber, normalized),
    );
    const [row] = await db
      .select({ id: bulkPaymentItems.id })
      .from(bulkPaymentItems)
      .innerJoin(bulkPaymentBatches, eq(bulkPaymentItems.batchId, bulkPaymentBatches.id))
      .where(
        and(
          batchCond,
          eq(bulkPaymentBatches.paymentType, "buy_goods"),
          eq(bulkPaymentItems.status, "completed"),
          tillMatch,
          sql`${bulkPaymentItems.mpesaTransactionId} IS NOT NULL AND trim(${bulkPaymentItems.mpesaTransactionId}) != ''`,
        ),
      )
      .limit(1);
    return Boolean(row?.id);
  }

  async getPaybillRecipientName(businessId: string, paybillNumber: string, accountNumber?: string): Promise<string | null> {
    const normalizedPaybill = String(paybillNumber ?? "").replace(/\D/g, "").slice(0, 7);
    if (!normalizedPaybill || normalizedPaybill.length < 5) return null;
    const batchCond = sql`(
      ${bulkPaymentBatches.createdByUserId} = (SELECT user_id FROM businesses WHERE id = ${businessId})
      OR ${bulkPaymentBatches.createdByUserId} IN (SELECT id FROM users WHERE business_id = ${businessId})
      OR ${bulkPaymentBatches.createdByUserId} IN (
        SELECT id FROM users
        WHERE created_by = (SELECT user_id FROM businesses WHERE id = ${businessId})
          AND business_id IS NULL
      )
    )`;
    const conditions = [
      batchCond,
      eq(bulkPaymentBatches.paymentType, "paybill"),
      eq(bulkPaymentItems.recipient, normalizedPaybill),
      sql`${bulkPaymentItems.recipientNameFromMpesa} IS NOT NULL AND ${bulkPaymentItems.recipientNameFromMpesa} != ''`,
    ];
    if (accountNumber != null && String(accountNumber).trim() !== "") {
      conditions.push(eq(bulkPaymentItems.accountNumber, String(accountNumber).trim()));
    }
    const [row] = await db
      .select({ name: bulkPaymentItems.recipientNameFromMpesa })
      .from(bulkPaymentItems)
      .innerJoin(bulkPaymentBatches, eq(bulkPaymentItems.batchId, bulkPaymentBatches.id))
      .where(and(...conditions))
      .orderBy(desc(bulkPaymentItems.processedAt))
      .limit(1);
    return row?.name ?? null;
  }

  async getUserWalletBalance(
    userId: string,
    opts?: { workspaceBusinessId?: string | null },
  ): Promise<{
    currentBalance: string;
    reservedDisbursements: string;
    availableBalance: string;
    overdraftLimit: string | null;
    overdraftUsed: string;
    overdraftRemaining: string;
    effectiveAvailable: string;
    overdraftDrawnAt: Date | null;
    hasApprovedCashAdvance: boolean;
  }> {
    const actor = await this.getUserById(userId);
    const ownedBusiness = await this.getBusinessByUserId(userId);
    const ws = opts?.workspaceBusinessId?.trim();
    const resolvedBusinessId =
      ws && ws.length > 0 ? ws : actor?.businessId || ownedBusiness?.id || null;
    let businessOwnerUserId = ownedBusiness?.userId || null;
    // Workspace switch / multi-company: scope wallet to session company even when user's primary row differs.
    if (resolvedBusinessId && (!businessOwnerUserId || ownedBusiness?.id !== resolvedBusinessId)) {
      const biz = await this.getBusinessById(resolvedBusinessId);
      businessOwnerUserId = biz?.userId || businessOwnerUserId;
    }

    const companyUserFilter = resolvedBusinessId
      ? sql`(
          ${walletTopups.userId} IN (SELECT ${users.id} FROM ${users} WHERE ${users.businessId} = ${resolvedBusinessId})
          ${businessOwnerUserId ? sql` OR ${walletTopups.userId} = ${businessOwnerUserId}` : sql``}
        )`
      : sql`${walletTopups.userId} = ${userId}`;

    const [approvedTopups] = await db
      .select({
        total: sql<string>`COALESCE(SUM(${walletTopups.amount}::numeric), 0)::text`,
      })
      .from(walletTopups)
      .where(
        and(
          companyUserFilter,
          eq(walletTopups.status, "approved"),
        ),
      );

    const paymentLinkUserFilter = resolvedBusinessId
      ? sql`(
          ${paymentLinks.createdByUserId} IN (SELECT ${users.id} FROM ${users} WHERE ${users.businessId} = ${resolvedBusinessId})
          ${businessOwnerUserId ? sql` OR ${paymentLinks.createdByUserId} = ${businessOwnerUserId}` : sql``}
        )`
      : sql`${paymentLinks.createdByUserId} = ${userId}`;

    const [paidLinks] = await db
      .select({
        total: sql<string>`COALESCE(SUM(COALESCE(${paymentLinks.paidAmount}::numeric, ${paymentLinks.amount}::numeric)), 0)::text`,
      })
      .from(paymentLinks)
      .where(
        and(
          paymentLinkUserFilter,
          eq(paymentLinks.status, "paid"),
        ),
      );

    const batchCreatorFilter = resolvedBusinessId
      ? sql`(
          ${bulkPaymentBatches.createdByUserId} IN (SELECT ${users.id} FROM ${users} WHERE ${users.businessId} = ${resolvedBusinessId})
          ${businessOwnerUserId ? sql` OR ${bulkPaymentBatches.createdByUserId} = ${businessOwnerUserId}` : sql``}
          OR (${bulkPaymentBatches.walletBusinessId} IS NOT NULL AND ${bulkPaymentBatches.walletBusinessId} = ${resolvedBusinessId})
        )`
      : sql`${bulkPaymentBatches.createdByUserId} = ${userId}`;

    const [completedDisbursements] = await db
      .select({
        total: sql<string>`COALESCE(SUM(COALESCE(${bulkPaymentItems.amount}::numeric, 0) + COALESCE(${bulkPaymentItems.fee}::numeric, 0)), 0)::text`,
      })
      .from(bulkPaymentItems)
      .innerJoin(bulkPaymentBatches, eq(bulkPaymentItems.batchId, bulkPaymentBatches.id))
      .where(
        and(
          batchCreatorFilter,
          eq(bulkPaymentItems.status, "completed"),
        ),
      );

    const inflows = Number(approvedTopups?.total || "0") + Number(paidLinks?.total || "0");
    const settledOutflows = Number(completedDisbursements?.total || "0");
    // Allow raw balance to be negative (overdraft usage)
    const currentBalance = inflows - settledOutflows;

    // Reserve queued/in-flight disbursements for this user to prevent over-queuing.
    const [reserved] = await db
      .select({
        total: sql<string>`COALESCE(SUM(COALESCE(${bulkPaymentItems.amount}::numeric, 0) + COALESCE(${bulkPaymentItems.fee}::numeric, 0)), 0)::text`,
      })
      .from(bulkPaymentItems)
      .innerJoin(bulkPaymentBatches, eq(bulkPaymentItems.batchId, bulkPaymentBatches.id))
      .where(
        and(
          batchCreatorFilter,
          sql`${bulkPaymentItems.status} IN ('pending', 'processing')`,
        ),
      );

    const reservedDisbursements = Number(reserved?.total || "0");
    const availableBalance = currentBalance - reservedDisbursements;

    // Fetch approved overdraft limit for this business
    let overdraftLimitNum: number | null = null;
    let overdraftDrawnAt: Date | null = null;
    if (resolvedBusinessId) {
      const overdraft = await this.getOverdraftLimitByBusinessId(resolvedBusinessId);
      if (overdraft?.status === "approved" && overdraft.approvedLimit) {
        overdraftLimitNum = Number(overdraft.approvedLimit);
        overdraftDrawnAt = overdraft.drawnAt ?? null;
      }
    }

    const overdraftUsed = overdraftLimitNum !== null ? Math.max(0, -availableBalance) : 0;
    const overdraftRemaining = overdraftLimitNum !== null ? Math.max(0, overdraftLimitNum - overdraftUsed) : 0;
    const effectiveAvailable = availableBalance + overdraftRemaining;

    let hasApprovedCashAdvance = false;
    if (resolvedBusinessId) {
      const advances = await this.getAdvancesByBusinessId(resolvedBusinessId);
      hasApprovedCashAdvance = advances.some((a) => a.status === "approved" || a.status === "active");
    }

    return {
      currentBalance: currentBalance.toFixed(2),
      reservedDisbursements: reservedDisbursements.toFixed(2),
      availableBalance: availableBalance.toFixed(2),
      overdraftLimit: overdraftLimitNum !== null ? overdraftLimitNum.toFixed(2) : null,
      overdraftUsed: overdraftUsed.toFixed(2),
      overdraftRemaining: overdraftRemaining.toFixed(2),
      effectiveAvailable: effectiveAvailable.toFixed(2),
      overdraftDrawnAt,
      hasApprovedCashAdvance,
    };
  }

  async ensureEmployeeWalletAccount(input: {
    businessId: string;
    employeeName: string;
    employeeId?: string | null;
    userId?: string | null;
  }): Promise<EmployeeWalletAccount> {
    const employeeId = input.employeeId?.trim() || null;
    const userId = input.userId?.trim() || null;

    if (userId) {
      const [byUser] = await db
        .select()
        .from(employeeWalletAccounts)
        .where(eq(employeeWalletAccounts.userId, userId))
        .limit(1);
      if (byUser) return byUser;
    }
    if (employeeId) {
      const [byEmployee] = await db
        .select()
        .from(employeeWalletAccounts)
        .where(and(eq(employeeWalletAccounts.businessId, input.businessId), eq(employeeWalletAccounts.employeeId, employeeId)))
        .limit(1);
      if (byEmployee) return byEmployee;
    }

    const prefix = this.twoLettersFromInitials(input.employeeName);
    const nextVal = await this.nextEmployeeWalletNumberSequence();
    const walletNumber = `${prefix}${String(nextVal).padStart(5, "0")}`;
    const [created] = await db
      .insert(employeeWalletAccounts)
      .values({
        businessId: input.businessId,
        userId,
        employeeId,
        employeeName: input.employeeName,
        walletNumber,
        status: "active",
      } satisfies InsertEmployeeWalletAccount)
      .returning();
    return created;
  }

  async getEmployeeWalletAccountForUser(userId: string): Promise<EmployeeWalletAccount | undefined> {
    const [row] = await db
      .select()
      .from(employeeWalletAccounts)
      .where(eq(employeeWalletAccounts.userId, userId))
      .limit(1);
    return row;
  }

  async getEmployeeWalletStatementForUser(
    userId: string,
    opts: { page: number; limit: number }
  ): Promise<{ wallet: EmployeeWalletAccount; entries: EmployeeWalletLedgerEntry[]; total: number; balance: string } | null> {
    const wallet = await this.getEmployeeWalletAccountForUser(userId);
    if (!wallet) return null;
    const page = Math.max(1, opts.page || 1);
    const limit = Math.min(100, Math.max(1, opts.limit || 20));
    const offset = (page - 1) * limit;

    const [entries, [countRow], [balanceRow]] = await Promise.all([
      db
        .select()
        .from(employeeWalletLedgerEntries)
        .where(eq(employeeWalletLedgerEntries.walletId, wallet.id))
        .orderBy(desc(employeeWalletLedgerEntries.createdAt))
        .limit(limit)
        .offset(offset),
      db
        .select({ count: sql<number>`count(*)::int` })
        .from(employeeWalletLedgerEntries)
        .where(eq(employeeWalletLedgerEntries.walletId, wallet.id)),
      db
        .select({
          balance: sql<string>`COALESCE(SUM(CASE WHEN ${employeeWalletLedgerEntries.direction} = 'credit' THEN ${employeeWalletLedgerEntries.amount}::numeric ELSE -${employeeWalletLedgerEntries.amount}::numeric END), 0)::text`,
        })
        .from(employeeWalletLedgerEntries)
        .where(eq(employeeWalletLedgerEntries.walletId, wallet.id)),
    ]);

    return { wallet, entries, total: countRow?.count ?? 0, balance: balanceRow?.balance ?? "0" };
  }

  async getEmployeeWalletBalanceForUser(userId: string): Promise<{ wallet: EmployeeWalletAccount; balance: string } | null> {
    const wallet = await this.getEmployeeWalletAccountForUser(userId);
    if (!wallet) return null;
    const [balanceRow] = await db
      .select({
        balance: sql<string>`COALESCE(SUM(CASE WHEN ${employeeWalletLedgerEntries.direction} = 'credit' THEN ${employeeWalletLedgerEntries.amount}::numeric ELSE -${employeeWalletLedgerEntries.amount}::numeric END), 0)::text`,
      })
      .from(employeeWalletLedgerEntries)
      .where(eq(employeeWalletLedgerEntries.walletId, wallet.id));
    return { wallet, balance: balanceRow?.balance ?? "0" };
  }

  async debitEmployeeWallet(input: {
    userId: string;
    businessId: string;
    amount: string;
    entryType: string;
    sourceRef?: string | null;
    description?: string | null;
    meta?: Record<string, unknown>;
  }): Promise<{ wallet: EmployeeWalletAccount; entry: EmployeeWalletLedgerEntry; balance: string }> {
    const current = await this.getEmployeeWalletBalanceForUser(input.userId);
    if (!current) throw new Error("Employee wallet not found");
    const currentBal = Number(current.balance || "0");
    const amt = Number(input.amount || "0");
    if (!Number.isFinite(amt) || amt <= 0) throw new Error("Invalid amount");
    if (currentBal < amt) throw new Error("Insufficient employee wallet balance");

    const [entry] = await db
      .insert(employeeWalletLedgerEntries)
      .values({
        walletId: current.wallet.id,
        businessId: input.businessId,
        direction: "debit",
        amount: input.amount,
        entryType: input.entryType,
        sourceRef: input.sourceRef ?? null,
        description: input.description ?? null,
        meta: input.meta ?? null,
        createdBy: input.userId,
      } satisfies InsertEmployeeWalletLedgerEntry)
      .returning();

    const [balanceRow] = await db
      .select({
        balance: sql<string>`COALESCE(SUM(CASE WHEN ${employeeWalletLedgerEntries.direction} = 'credit' THEN ${employeeWalletLedgerEntries.amount}::numeric ELSE -${employeeWalletLedgerEntries.amount}::numeric END), 0)::text`,
      })
      .from(employeeWalletLedgerEntries)
      .where(eq(employeeWalletLedgerEntries.walletId, current.wallet.id));

    return { wallet: current.wallet, entry, balance: balanceRow?.balance ?? "0" };
  }

  async releaseEmployeeWalletReserveForItem(input: {
    itemId: string;
    reason: string;
    releaseType: "payout_refund" | "payout_scheduled_release";
  }): Promise<boolean> {
    const [debit] = await db
      .select()
      .from(employeeWalletLedgerEntries)
      .where(
        and(
          eq(employeeWalletLedgerEntries.sourceRef, input.itemId),
          eq(employeeWalletLedgerEntries.direction, "debit"),
          inArray(employeeWalletLedgerEntries.entryType, ["payout_debit", "payout_scheduled_reserve"]),
        ),
      )
      .orderBy(desc(employeeWalletLedgerEntries.createdAt))
      .limit(1);
    if (!debit) return false;

    const [alreadyReleased] = await db
      .select({ id: employeeWalletLedgerEntries.id })
      .from(employeeWalletLedgerEntries)
      .where(
        and(
          eq(employeeWalletLedgerEntries.sourceRef, input.itemId),
          eq(employeeWalletLedgerEntries.direction, "credit"),
          inArray(employeeWalletLedgerEntries.entryType, ["payout_refund", "payout_scheduled_release"]),
        ),
      )
      .limit(1);
    if (alreadyReleased) return false;

    await db.insert(employeeWalletLedgerEntries).values({
      walletId: debit.walletId,
      businessId: debit.businessId,
      direction: "credit",
      amount: debit.amount,
      entryType: input.releaseType,
      sourceRef: input.itemId,
      description: input.reason,
      meta: { releaseOfEntryId: debit.id },
      createdBy: debit.createdBy,
    } satisfies InsertEmployeeWalletLedgerEntry);
    return true;
  }

  async reapplyWalletDebitAfterErroneousPayoutRelease(itemId: string, reason: string): Promise<boolean> {
    const [already] = await db
      .select({ id: employeeWalletLedgerEntries.id })
      .from(employeeWalletLedgerEntries)
      .where(
        and(
          eq(employeeWalletLedgerEntries.sourceRef, itemId),
          eq(employeeWalletLedgerEntries.direction, "debit"),
          eq(employeeWalletLedgerEntries.entryType, "payout_reconciliation_debit"),
        ),
      )
      .limit(1);
    if (already) return false;

    const [refund] = await db
      .select()
      .from(employeeWalletLedgerEntries)
      .where(
        and(
          eq(employeeWalletLedgerEntries.sourceRef, itemId),
          eq(employeeWalletLedgerEntries.direction, "credit"),
          inArray(employeeWalletLedgerEntries.entryType, ["payout_refund", "payout_scheduled_release"]),
        ),
      )
      .orderBy(desc(employeeWalletLedgerEntries.createdAt))
      .limit(1);
    if (!refund) return false;

    await db.insert(employeeWalletLedgerEntries).values({
      walletId: refund.walletId,
      businessId: refund.businessId,
      direction: "debit",
      amount: refund.amount,
      entryType: "payout_reconciliation_debit",
      sourceRef: itemId,
      description: reason,
      meta: { reversesReleaseEntryId: refund.id },
      createdBy: refund.createdBy,
    } satisfies InsertEmployeeWalletLedgerEntry);
    return true;
  }

  async getEmployeeWalletReserveExceptions(input: {
    businessId: string;
    limit?: number;
    graceMinutes?: number;
  }): Promise<{
    scanned: number;
    exceptions: Array<{
      itemId: string | null;
      batchId: string | null;
      walletId: string;
      debitEntryId: string;
      amount: string;
      entryType: string;
      itemStatus: string | null;
      scheduledFor: string | null;
      releasePresent: boolean;
      category:
        | "orphan_debit_no_item"
        | "failed_without_release"
        | "stuck_without_resolution"
        | "completed_but_released_conflict";
      debitCreatedAt: string;
      ageMinutes: number;
    }>;
  }> {
    const limit = Math.min(1000, Math.max(20, Number(input.limit ?? 300)));
    const graceMinutes = Math.min(24 * 60, Math.max(10, Number(input.graceMinutes ?? 120)));
    const now = Date.now();

    const debits = await db
      .select({
        id: employeeWalletLedgerEntries.id,
        walletId: employeeWalletLedgerEntries.walletId,
        amount: employeeWalletLedgerEntries.amount,
        entryType: employeeWalletLedgerEntries.entryType,
        sourceRef: employeeWalletLedgerEntries.sourceRef,
        createdAt: employeeWalletLedgerEntries.createdAt,
      })
      .from(employeeWalletLedgerEntries)
      .where(
        and(
          eq(employeeWalletLedgerEntries.businessId, input.businessId),
          eq(employeeWalletLedgerEntries.direction, "debit"),
          inArray(employeeWalletLedgerEntries.entryType, ["payout_debit", "payout_scheduled_reserve"]),
          sql`${employeeWalletLedgerEntries.sourceRef} IS NOT NULL`,
        ),
      )
      .orderBy(desc(employeeWalletLedgerEntries.createdAt))
      .limit(limit);

    const itemIds = Array.from(
      new Set(
        debits
          .map((d) => d.sourceRef || "")
          .filter((v): v is string => Boolean(v)),
      ),
    );

    const items = itemIds.length
      ? await db
        .select({
          id: bulkPaymentItems.id,
          batchId: bulkPaymentItems.batchId,
          status: bulkPaymentItems.status,
          scheduledFor: bulkPaymentBatches.scheduledFor,
        })
        .from(bulkPaymentItems)
        .innerJoin(bulkPaymentBatches, eq(bulkPaymentItems.batchId, bulkPaymentBatches.id))
        .where(inArray(bulkPaymentItems.id, itemIds))
      : [];

    const itemMap = new Map(items.map((i) => [i.id, i]));

    const releases = itemIds.length
      ? await db
        .select({
          sourceRef: employeeWalletLedgerEntries.sourceRef,
        })
        .from(employeeWalletLedgerEntries)
        .where(
          and(
            eq(employeeWalletLedgerEntries.businessId, input.businessId),
            eq(employeeWalletLedgerEntries.direction, "credit"),
            inArray(employeeWalletLedgerEntries.entryType, ["payout_refund", "payout_scheduled_release"]),
            inArray(employeeWalletLedgerEntries.sourceRef, itemIds),
          ),
        )
      : [];
    const releasedSet = new Set(releases.map((r) => String(r.sourceRef)));

    const exceptions: Array<{
      itemId: string | null;
      batchId: string | null;
      walletId: string;
      debitEntryId: string;
      amount: string;
      entryType: string;
      itemStatus: string | null;
      scheduledFor: string | null;
      releasePresent: boolean;
      category:
        | "orphan_debit_no_item"
        | "failed_without_release"
        | "stuck_without_resolution"
        | "completed_but_released_conflict";
      debitCreatedAt: string;
      ageMinutes: number;
    }> = [];

    for (const d of debits) {
      const sourceRef = d.sourceRef ? String(d.sourceRef) : null;
      const item = sourceRef ? itemMap.get(sourceRef) : undefined;
      const releasePresent = sourceRef ? releasedSet.has(sourceRef) : false;
      const createdAt = d.createdAt ? new Date(d.createdAt) : new Date();
      const ageMinutes = Math.max(0, Math.floor((now - createdAt.getTime()) / 60000));
      const scheduledFor = item?.scheduledFor ? new Date(item.scheduledFor as any) : null;
      const isFutureScheduled = !!(scheduledFor && scheduledFor.getTime() > now);

      let category:
        | "orphan_debit_no_item"
        | "failed_without_release"
        | "stuck_without_resolution"
        | "completed_but_released_conflict"
        | null = null;

      if (!item) {
        category = "orphan_debit_no_item";
      } else if (item.status === "failed" && !releasePresent) {
        category = "failed_without_release";
      } else if ((item.status === "pending" || item.status === "processing") && !isFutureScheduled && ageMinutes >= graceMinutes) {
        category = "stuck_without_resolution";
      } else if (item.status === "completed" && releasePresent) {
        category = "completed_but_released_conflict";
      }

      if (!category) continue;
      exceptions.push({
        itemId: sourceRef,
        batchId: item?.batchId ?? null,
        walletId: d.walletId,
        debitEntryId: d.id,
        amount: String(d.amount),
        entryType: String(d.entryType),
        itemStatus: item?.status ?? null,
        scheduledFor: scheduledFor ? scheduledFor.toISOString() : null,
        releasePresent,
        category,
        debitCreatedAt: createdAt.toISOString(),
        ageMinutes,
      });
    }

    return { scanned: debits.length, exceptions };
  }

  async creditEmployeeWalletFromExpense(input: {
    businessId: string;
    employeeName: string;
    employeeId?: string | null;
    userId?: string | null;
    amount: string;
    expenseClaimId: string;
    note?: string | null;
    actorUserId: string;
  }): Promise<{ wallet: EmployeeWalletAccount; entry: EmployeeWalletLedgerEntry; balance: string }> {
    const wallet = await this.ensureEmployeeWalletAccount({
      businessId: input.businessId,
      employeeName: input.employeeName,
      employeeId: input.employeeId ?? null,
      userId: input.userId ?? null,
    });
    const [entry] = await db
      .insert(employeeWalletLedgerEntries)
      .values({
        walletId: wallet.id,
        businessId: input.businessId,
        direction: "credit",
        amount: input.amount,
        entryType: "expense_reimbursement",
        sourceRef: input.expenseClaimId,
        description: input.note ?? `Expense reimbursement ${input.expenseClaimId}`,
        meta: { expenseClaimId: input.expenseClaimId },
        createdBy: input.actorUserId,
      } satisfies InsertEmployeeWalletLedgerEntry)
      .returning();

    const [balanceRow] = await db
      .select({
        balance: sql<string>`COALESCE(SUM(CASE WHEN ${employeeWalletLedgerEntries.direction} = 'credit' THEN ${employeeWalletLedgerEntries.amount}::numeric ELSE -${employeeWalletLedgerEntries.amount}::numeric END), 0)::text`,
      })
      .from(employeeWalletLedgerEntries)
      .where(eq(employeeWalletLedgerEntries.walletId, wallet.id));

    return { wallet, entry, balance: balanceRow?.balance ?? "0" };
  }

  async getEmployeeWalletFundingAnalytics(
    businessId: string,
    opts?: { dateFrom?: Date; dateTo?: Date }
  ): Promise<{ totalFunded: string; fundedCount: number; uniqueWallets: number }> {
    const conds: SQL[] = [
      eq(employeeWalletLedgerEntries.businessId, businessId),
      eq(employeeWalletLedgerEntries.entryType, "expense_reimbursement"),
      eq(employeeWalletLedgerEntries.direction, "credit"),
    ];
    if (opts?.dateFrom) conds.push(gte(employeeWalletLedgerEntries.createdAt, opts.dateFrom));
    if (opts?.dateTo) conds.push(lt(employeeWalletLedgerEntries.createdAt, opts.dateTo));
    const where = and(...conds)!;

    const [row] = await db
      .select({
        totalFunded: sql<string>`COALESCE(SUM(${employeeWalletLedgerEntries.amount}::numeric), 0)::text`,
        fundedCount: sql<number>`count(*)::int`,
        uniqueWallets: sql<number>`count(distinct ${employeeWalletLedgerEntries.walletId})::int`,
      })
      .from(employeeWalletLedgerEntries)
      .where(where);
    return {
      totalFunded: row?.totalFunded ?? "0",
      fundedCount: row?.fundedCount ?? 0,
      uniqueWallets: row?.uniqueWallets ?? 0,
    };
  }

  async createAdvance(advance: InsertCashAdvance): Promise<CashAdvance> {
    const [result] = await db.insert(cashAdvances).values({ ...advance, id: crypto.randomUUID() }).returning();
    return result;
  }

  async getAdvances(): Promise<CashAdvance[]> {
    return db.select().from(cashAdvances).orderBy(desc(cashAdvances.createdAt));
  }

  async getAdvancesByBusinessId(businessId: string): Promise<CashAdvance[]> {
    return db
      .select()
      .from(cashAdvances)
      .where(eq(cashAdvances.businessId, businessId))
      .orderBy(desc(cashAdvances.createdAt));
  }

  async getAdvancesExcludingSandbox(): Promise<CashAdvance[]> {
    return db
      .select({ a: cashAdvances })
      .from(cashAdvances)
      .innerJoin(businesses, eq(cashAdvances.businessId, businesses.id))
      .where(
        and(
          sql`COALESCE(${businesses.isSandbox}, false) = false`,
          sql`${cashAdvances.businessId} IS NOT NULL`,
          sql`COALESCE(${cashAdvances.businessId}, '') NOT LIKE 'demo-%'`,
        ),
      )
      .orderBy(desc(cashAdvances.createdAt))
      .then((rows) => rows.map((r) => r.a));
  }

  async getAdminAdvancesEnriched(opts: { limit: number; offset: number }): Promise<{ data: Array<CashAdvance & { business?: Business | null }>; total: number }> {
    const limit = Number.isFinite(opts.limit) ? Math.min(200, Math.max(1, opts.limit)) : 20;
    const offset = Number.isFinite(opts.offset) ? Math.max(0, opts.offset) : 0;

    const baseWhere = and(
      sql`COALESCE(${businesses.isSandbox}, false) = false`,
      sql`${cashAdvances.businessId} IS NOT NULL`,
      sql`COALESCE(${cashAdvances.businessId}, '') NOT LIKE 'demo-%'`,
    );

    const [{ count }] = await db
      .select({ count: sql<number>`COUNT(*)::int` })
      .from(cashAdvances)
      .innerJoin(businesses, eq(cashAdvances.businessId, businesses.id))
      .where(baseWhere);

    const rows = await db
      .select({ a: cashAdvances, b: businesses })
      .from(cashAdvances)
      .innerJoin(businesses, eq(cashAdvances.businessId, businesses.id))
      .where(baseWhere)
      .orderBy(desc(cashAdvances.createdAt))
      .limit(limit)
      .offset(offset);

    const data = rows.map((r) => ({ ...r.a, business: r.b ?? null }));
    return { data, total: Number(count || 0) };
  }

  async getAdminAdvancesOverview(): Promise<{
    totalRequested: string;
    totalActive: string;
    totalRepaid: string;
    pendingCount: number;
    countsByStatus: Array<{ status: string; count: number }>;
    monthly: Array<{ month: string; amount: string; count: number }>;
  }> {
    const baseWhere = and(
      sql`COALESCE(${businesses.isSandbox}, false) = false`,
      sql`${cashAdvances.businessId} IS NOT NULL`,
      sql`COALESCE(${cashAdvances.businessId}, '') NOT LIKE 'demo-%'`,
    );

    const [totals] = await db
      .select({
        totalRequested: sql<string>`COALESCE(SUM(${cashAdvances.amount}::numeric), 0)::text`,
        totalActive: sql<string>`COALESCE(SUM(CASE WHEN ${cashAdvances.status} = 'active' THEN ${cashAdvances.amount}::numeric ELSE 0 END), 0)::text`,
        totalRepaid: sql<string>`COALESCE(SUM(CASE WHEN ${cashAdvances.status} = 'repaid' THEN ${cashAdvances.totalRepayment}::numeric ELSE 0 END), 0)::text`,
        pendingCount: sql<number>`COALESCE(SUM(CASE WHEN ${cashAdvances.status} = 'pending' THEN 1 ELSE 0 END), 0)::int`,
      })
      .from(cashAdvances)
      .innerJoin(businesses, eq(cashAdvances.businessId, businesses.id))
      .where(baseWhere);

    const countsByStatus = await db
      .select({
        status: cashAdvances.status,
        count: sql<number>`COUNT(*)::int`,
      })
      .from(cashAdvances)
      .innerJoin(businesses, eq(cashAdvances.businessId, businesses.id))
      .where(baseWhere)
      .groupBy(cashAdvances.status)
      .orderBy(asc(cashAdvances.status));

    const monthly = await db
      .select({
        month: sql<string>`TO_CHAR(DATE_TRUNC('month', ${cashAdvances.createdAt}), 'Mon YYYY')`,
        amount: sql<string>`COALESCE(SUM(${cashAdvances.amount}::numeric), 0)::text`,
        count: sql<number>`COUNT(*)::int`,
      })
      .from(cashAdvances)
      .innerJoin(businesses, eq(cashAdvances.businessId, businesses.id))
      .where(baseWhere)
      .groupBy(sql`DATE_TRUNC('month', ${cashAdvances.createdAt})`)
      .orderBy(sql`DATE_TRUNC('month', ${cashAdvances.createdAt})`)
      .limit(6);

    return {
      totalRequested: totals?.totalRequested ?? "0",
      totalActive: totals?.totalActive ?? "0",
      totalRepaid: totals?.totalRepaid ?? "0",
      pendingCount: Number(totals?.pendingCount ?? 0),
      countsByStatus: countsByStatus.map((r) => ({ status: String(r.status), count: Number(r.count || 0) })),
      monthly: monthly.map((r) => ({ month: r.month, amount: r.amount, count: Number(r.count || 0) })),
    };
  }

  async getAdvance(id: string): Promise<CashAdvance | undefined> {
    const [result] = await db.select().from(cashAdvances).where(eq(cashAdvances.id, id));
    return result;
  }

  async updateAdvanceStatus(id: string, status: string): Promise<CashAdvance | undefined> {
    const [result] = await db.update(cashAdvances)
      .set({ status: status as any })
      .where(eq(cashAdvances.id, id))
      .returning();
    return result;
  }

  async reviewAdvance(
    id: string,
    status: string,
    reviewedBy: string,
    reviewerName: string,
    note?: string,
    approvedAmount?: string | null,
  ): Promise<CashAdvance | undefined> {
    const updates: any = {
      status: status as any,
      reviewedBy,
      reviewerName,
      reviewNote: note || null,
      reviewedAt: new Date(),
    };

    if (approvedAmount != null && (status === "approved" || status === "active")) {
      const amountNum = Number(approvedAmount);
      if (!Number.isNaN(amountNum) && amountNum > 0) {
        const [current] = await db.select().from(cashAdvances).where(eq(cashAdvances.id, id));
        const days = Number(current?.durationDays || 30);
        // Tiered interest rates from platform settings
        const rate1_10 = Number(await this.getPlatformSetting("cash_advance_rate_1_10") || "2.5");
        const rate11_20 = Number(await this.getPlatformSetting("cash_advance_rate_11_20") || "5");
        const rate21_30 = Number(await this.getPlatformSetting("cash_advance_rate_21_30") || "7.5");
        const rate = days <= 10 ? rate1_10 : days <= 20 ? rate11_20 : rate21_30;
        const totalRepayment = amountNum + amountNum * (rate / 100);
        updates.amount = amountNum.toFixed(2);
        updates.interestRate = rate.toFixed(2);
        updates.totalRepayment = totalRepayment.toFixed(2);
      }
    }

    const [result] = await db.update(cashAdvances)
      .set(updates)
      .where(eq(cashAdvances.id, id))
      .returning();
    return result;
  }

  async createOverdraftLimit(limit: InsertOverdraftLimit): Promise<OverdraftLimit> {
    const [result] = await db.insert(overdraftLimits).values({ ...limit, id: crypto.randomUUID() }).returning();
    return result;
  }

  async getOverdraftLimits(): Promise<OverdraftLimit[]> {
    return db.select().from(overdraftLimits).orderBy(desc(overdraftLimits.createdAt));
  }

  async getOverdraftLimitsPaginated(limit: number, offset: number): Promise<{ data: OverdraftLimit[]; total: number }> {
    const [data, totalResult] = await Promise.all([
      db.select().from(overdraftLimits).orderBy(desc(overdraftLimits.createdAt)).limit(limit).offset(offset),
      db.select({ count: sql<number>`count(*)::int` }).from(overdraftLimits),
    ]);
    return { data, total: totalResult[0]?.count ?? 0 };
  }

  async getOverdraftLimitsOverview(): Promise<{ totalRequested: string; totalApproved: string; totalCurrentBalance: string; pendingCount: number; approvedCount: number; utilization: string; countsByStatus: Array<{ status: string; count: number }> }> {
    const rows = await db.select({
      status: overdraftLimits.status,
      requestedLimit: overdraftLimits.requestedLimit,
      approvedLimit: overdraftLimits.approvedLimit,
      currentBalance: overdraftLimits.currentBalance,
    }).from(overdraftLimits);

    const totalRequested = rows.reduce((s, r) => s + parseFloat(r.requestedLimit || "0"), 0);
    const totalApproved = rows.filter(r => r.status === "approved").reduce((s, r) => s + parseFloat(r.approvedLimit || "0"), 0);
    const totalCurrentBalance = rows.filter(r => r.status === "approved").reduce((s, r) => s + parseFloat(r.currentBalance || "0"), 0);
    const utilization = totalApproved > 0 ? ((totalCurrentBalance / totalApproved) * 100).toFixed(1) : "0";
    const statusCounts: Record<string, number> = {};
    rows.forEach(r => { statusCounts[r.status] = (statusCounts[r.status] || 0) + 1; });

    return {
      totalRequested: String(totalRequested),
      totalApproved: String(totalApproved),
      totalCurrentBalance: String(totalCurrentBalance),
      pendingCount: statusCounts["pending"] || 0,
      approvedCount: statusCounts["approved"] || 0,
      utilization,
      countsByStatus: Object.entries(statusCounts).map(([status, count]) => ({ status, count })),
    };
  }

  async getOverdraftLimitById(id: string): Promise<OverdraftLimit | undefined> {
    const [result] = await db.select().from(overdraftLimits).where(eq(overdraftLimits.id, id));
    return result;
  }

  async getOverdraftLimitByBusinessId(businessId: string): Promise<OverdraftLimit | undefined> {
    const [result] = await db.select().from(overdraftLimits)
      .where(eq(overdraftLimits.businessId, businessId))
      .orderBy(desc(overdraftLimits.createdAt))
      .limit(1);
    return result;
  }

  async getOverdraftLimitsByBusinessId(businessId: string): Promise<OverdraftLimit[]> {
    return db.select().from(overdraftLimits)
      .where(eq(overdraftLimits.businessId, businessId))
      .orderBy(desc(overdraftLimits.createdAt));
  }

  async reviewOverdraftLimit(id: string, status: string, approvedLimit: string | null, reviewedBy: string, reviewerName: string, note?: string): Promise<OverdraftLimit | undefined> {
    const [result] = await db.update(overdraftLimits)
      .set({
        status: status as any,
        approvedLimit,
        reviewedBy,
        reviewerName,
        reviewNote: note || null,
        reviewedAt: new Date(),
      })
      .where(eq(overdraftLimits.id, id))
      .returning();
    return result;
  }

  // ── Overdraft tracking ──────────────────────────────────────────────────────
  async updateOverdraftDrawn(businessId: string, currentBalance: string, drawnAt: Date): Promise<void> {
    await db.update(overdraftLimits)
      .set({ currentBalance, drawnAt })
      .where(and(eq(overdraftLimits.businessId, businessId), eq(overdraftLimits.status, "approved")));
  }

  async clearOverdraftBalance(businessId: string): Promise<void> {
    await db.update(overdraftLimits)
      .set({ currentBalance: "0", drawnAt: null, interestAccrued: "0" })
      .where(and(eq(overdraftLimits.businessId, businessId), eq(overdraftLimits.status, "approved")));
  }

  async partialRepayOverdraft(businessId: string, newBalance: string): Promise<void> {
    await db.update(overdraftLimits)
      .set({ currentBalance: newBalance, drawnAt: new Date() }) // reset interest clock
      .where(and(eq(overdraftLimits.businessId, businessId), eq(overdraftLimits.status, "approved")));
  }

  // ── Platform settings ────────────────────────────────────────────────────────
  async getPlatformSettings(): Promise<PlatformSetting[]> {
    return db.select().from(platformSettings);
  }

  async getPlatformSetting(key: string): Promise<string | null> {
    const [row] = await db.select().from(platformSettings).where(eq(platformSettings.key, key));
    return row?.value ?? null;
  }

  async setPlatformSetting(key: string, value: string, updatedByName?: string): Promise<void> {
    await db.insert(platformSettings)
      .values({ key, value, updatedAt: new Date(), updatedByName: updatedByName ?? null })
      .onConflictDoUpdate({
        target: platformSettings.key,
        set: { value, updatedAt: new Date(), updatedByName: updatedByName ?? null },
      });
  }

  async createInvestorProfile(profile: InsertInvestorProfile): Promise<InvestorProfile> {
    const [result] = await db.insert(investorProfiles).values({ ...profile, id: crypto.randomUUID() }).returning();
    return result;
  }

  async getInvestorProfiles(): Promise<InvestorProfile[]> {
    return db.select().from(investorProfiles).orderBy(desc(investorProfiles.createdAt));
  }

  async getInvestorProfileById(id: string): Promise<InvestorProfile | undefined> {
    const [result] = await db.select().from(investorProfiles).where(eq(investorProfiles.id, id));
    return result;
  }

  async updateInvestorProfile(id: string, data: Partial<InsertInvestorProfile>): Promise<InvestorProfile | undefined> {
    const [result] = await db.update(investorProfiles).set(data).where(eq(investorProfiles.id, id)).returning();
    return result;
  }

  async updateInvestorKycStatus(id: string, status: string): Promise<InvestorProfile | undefined> {
    const [result] = await db.update(investorProfiles).set({ kycStatus: status as any }).where(eq(investorProfiles.id, id)).returning();
    return result;
  }

  async createInvestment(investment: InsertInvestment): Promise<Investment> {
    const [result] = await db.insert(investments).values({ ...investment, id: crypto.randomUUID() }).returning();
    return result;
  }

  async getInvestments(): Promise<Investment[]> {
    return db.select().from(investments).orderBy(desc(investments.createdAt));
  }

  async getInvestment(id: string): Promise<Investment | undefined> {
    const [result] = await db.select().from(investments).where(eq(investments.id, id));
    return result;
  }

  async createTransaction(transaction: InsertTransaction): Promise<Transaction> {
    const id = crypto.randomUUID();
    const ref = (transaction.reference || "").trim();
    if (ref) {
      await db
        .insert(transactions)
        .values({ ...transaction, id })
        .onConflictDoNothing({ target: transactions.reference });
      const existing = await this.findTransactionByReference(ref);
      if (existing) return existing;
    }
    const [result] = await db.insert(transactions).values({ ...transaction, id }).returning();
    return result;
  }

  async findTransactionByReference(reference: string): Promise<Transaction | undefined> {
    const ref = (reference || "").trim();
    if (!ref) return undefined;
    const [row] = await db.select().from(transactions).where(eq(transactions.reference, ref)).limit(1);
    return row;
  }

  async getTransactions(limit = 20): Promise<Transaction[]> {
    return db.select().from(transactions).orderBy(desc(transactions.createdAt)).limit(limit);
  }

  async createSavedTemplate(template: InsertSavedTemplate): Promise<SavedTemplate> {
    const [result] = await db.insert(savedTemplates).values({ ...template, id: crypto.randomUUID() }).returning();
    return result;
  }

  async getSavedTemplates(userId?: string, businessId?: string): Promise<SavedTemplate[]> {
    // Prefer business-scoped query so all team members see shared templates
    if (businessId) {
      return db
        .select()
        .from(savedTemplates)
        .where(eq(savedTemplates.businessId, businessId))
        .orderBy(desc(savedTemplates.createdAt));
    }
    if (userId) {
      return db
        .select()
        .from(savedTemplates)
        .where(eq(savedTemplates.userId, userId))
        .orderBy(desc(savedTemplates.createdAt));
    }
    return db.select().from(savedTemplates).orderBy(desc(savedTemplates.createdAt));
  }

  async updateSavedTemplate(
    id: string,
    userId: string,
    businessId: string | undefined,
    data: Partial<Pick<SavedTemplate, "name" | "description" | "type" | "data" | "recipientCount">>
  ): Promise<SavedTemplate | undefined> {
    // Allow update if the template belongs to the same business OR was created by this user
    const ownerFilter = businessId
      ? or(eq(savedTemplates.businessId, businessId), eq(savedTemplates.userId, userId))
      : eq(savedTemplates.userId, userId);
    const [updated] = await db
      .update(savedTemplates)
      .set(data)
      .where(and(eq(savedTemplates.id, id), ownerFilter))
      .returning();
    return updated;
  }

  async deleteSavedTemplate(id: string, userId?: string, businessId?: string): Promise<void> {
    const ownerFilter = businessId
      ? or(eq(savedTemplates.businessId, businessId), eq(savedTemplates.userId, userId!))
      : userId
        ? eq(savedTemplates.userId, userId)
        : undefined;
    if (ownerFilter) {
      await db.delete(savedTemplates).where(and(eq(savedTemplates.id, id), ownerFilter));
      return;
    }
    await db.delete(savedTemplates).where(eq(savedTemplates.id, id));
  }

  async createRecurringPayment(payment: InsertRecurringPayment): Promise<RecurringPayment> {
    const [result] = await db.insert(recurringPayments).values({ ...payment, id: crypto.randomUUID() }).returning();
    return result;
  }

  async getRecurringPayments(): Promise<RecurringPayment[]> {
    return db.select().from(recurringPayments).orderBy(desc(recurringPayments.createdAt));
  }

  async getRecurringPaymentsByBusiness(businessId: string): Promise<RecurringPayment[]> {
    return db.select().from(recurringPayments)
      .where(eq(recurringPayments.businessId, businessId))
      .orderBy(desc(recurringPayments.createdAt));
  }

  async getRecurringPaymentsByUser(userId: string): Promise<RecurringPayment[]> {
    return db.select().from(recurringPayments)
      .where(eq(recurringPayments.userId, userId))
      .orderBy(desc(recurringPayments.createdAt));
  }

  async getDueRecurringPayments(): Promise<RecurringPayment[]> {
    return db.select().from(recurringPayments).where(
      and(
        eq(recurringPayments.status, "active"),
        sql`${recurringPayments.nextPaymentDate} <= NOW()`,
        or(
          sql`${recurringPayments.endDate} IS NULL`,
          sql`${recurringPayments.endDate} > NOW()`
        ),
        or(
          sql`${recurringPayments.maxExecutions} IS NULL`,
          sql`${recurringPayments.totalExecutions} < ${recurringPayments.maxExecutions}`
        )
      )
    );
  }

  async advanceRecurringPayment(id: string, nextDate: Date): Promise<void> {
    await db.update(recurringPayments).set({
      nextPaymentDate: nextDate,
      lastExecutedAt: new Date(),
      totalExecutions: sql`${recurringPayments.totalExecutions} + 1`,
    }).where(eq(recurringPayments.id, id));
  }

  async updateRecurringPaymentStatus(id: string, status: string): Promise<RecurringPayment | undefined> {
    const [result] = await db.update(recurringPayments)
      .set({ status })
      .where(eq(recurringPayments.id, id))
      .returning();
    return result;
  }

  async updateRecurringPayment(id: string, data: Partial<{ name: string; description: string | null; frequency: string; dayOfWeek: number | null; dayOfMonth: number | null; nextPaymentDate: Date; endDate: Date | null; maxExecutions: number | null; amount: string; recipient: string; accountNumber: string | null; }>): Promise<RecurringPayment | undefined> {
    const [result] = await db.update(recurringPayments).set(data).where(eq(recurringPayments.id, id)).returning();
    return result;
  }

  async deleteRecurringPayment(id: string): Promise<void> {
    await db.delete(recurringPayments).where(eq(recurringPayments.id, id));
  }

  async getRecurringPaymentExecutionHistory(rpId: string, limit = 30) {
    const batches = await db
      .select()
      .from(bulkPaymentBatches)
      .where(like(bulkPaymentBatches.createdByName, `Recurring:${rpId}|%`))
      .orderBy(desc(bulkPaymentBatches.createdAt))
      .limit(limit);

    return Promise.all(
      batches.map(async (batch) => {
        const items = await db
          .select({
            id: bulkPaymentItems.id,
            recipient: bulkPaymentItems.recipient,
            amount: bulkPaymentItems.amount,
            fee: bulkPaymentItems.fee,
            status: bulkPaymentItems.status,
            failureReason: bulkPaymentItems.failureReason,
          })
          .from(bulkPaymentItems)
          .where(eq(bulkPaymentItems.batchId, batch.id));

        return {
          batchId: batch.id,
          status: batch.status,
          totalAmount: batch.totalAmount,
          totalFees: batch.totalFees,
          recipientCount: batch.recipientCount,
          completedCount: batch.completedCount ?? 0,
          failedCount: batch.failedCount ?? 0,
          createdAt: batch.createdAt,
          items,
        };
      })
    );
  }

  async getRecurringPaymentById(id: string): Promise<RecurringPayment | undefined> {
    const [result] = await db.select().from(recurringPayments).where(eq(recurringPayments.id, id));
    return result;
  }

  async updateRecurringPaymentApproval(
    id: string,
    approvalStatus: string,
    approvedBy?: string,
    approvedByName?: string,
    approvedAt?: Date,
    approvalId?: string
  ): Promise<RecurringPayment | undefined> {
    const updateData: Record<string, unknown> = { approvalStatus };
    if (approvedBy !== undefined) updateData.approvedBy = approvedBy;
    if (approvedByName !== undefined) updateData.approvedByName = approvedByName;
    if (approvedAt !== undefined) updateData.approvedAt = approvedAt;
    if (approvalId !== undefined) updateData.approvalId = approvalId;
    const [result] = await db.update(recurringPayments)
      .set(updateData as any)
      .where(eq(recurringPayments.id, id))
      .returning();
    return result;
  }

  async getUsersByRoleInBusiness(businessId: string, roles: string[]): Promise<{ id: string; email: string; name: string }[]> {
    // Members whose primary role matches OR who have the role via the junction table
    const primaryMembers = await db
      .select({ id: users.id, email: users.email, name: users.fullName })
      .from(users)
      .where(and(eq(users.businessId, businessId), inArray(users.role, roles as any[])));

    const linkedPrimaryMembers = await db
      .selectDistinct({ id: users.id, email: users.email, name: users.fullName })
      .from(userBusinessMemberships)
      .innerJoin(users, eq(userBusinessMemberships.userId, users.id))
      .where(and(eq(userBusinessMemberships.businessId, businessId), inArray(users.role, roles as any[])));

    let junctionMembers: { id: string; email: string; name: string }[] = [];
    try {
      junctionMembers = await db
        .selectDistinct({ id: users.id, email: users.email, name: users.fullName })
        .from(userPlatformRoles)
        .innerJoin(users, eq(userPlatformRoles.userId, users.id))
        .where(and(eq(users.businessId, businessId), inArray(userPlatformRoles.roleSlug, roles)));
    } catch (e) {
      const msg = String((e as { message?: string })?.message ?? "");
      if (msg.includes("user_platform_roles") && msg.includes("does not exist")) {
        console.warn("[storage] user_platform_roles table missing; falling back to primary roles only");
      } else {
        throw e;
      }
    }

    const seen = new Set<string>();
    const members: { id: string; email: string; name: string }[] = [];
    for (const m of [...primaryMembers, ...linkedPrimaryMembers, ...junctionMembers]) {
      if (!seen.has(m.id)) { seen.add(m.id); members.push(m); }
    }

    // Also include the business owner — their users.businessId may be null
    // because they own via businesses.userId rather than being a member
    const [biz] = await db.select().from(businesses).where(eq(businesses.id, businessId));
    if (biz?.userId && !seen.has(biz.userId)) {
      const [owner] = await db.select().from(users).where(eq(users.id, biz.userId));
      if (owner) members.push({ id: owner.id, email: owner.email, name: owner.fullName });
    }

    return members;
  }

  async getUserRoles(userId: string): Promise<string[]> {
    try {
      const rows = await db
        .select({ roleSlug: userPlatformRoles.roleSlug })
        .from(userPlatformRoles)
        .where(eq(userPlatformRoles.userId, userId));
      return rows.map((r) => r.roleSlug);
    } catch (e) {
      const msg = String((e as { message?: string })?.message ?? "");
      if (msg.includes("user_platform_roles") && msg.includes("does not exist")) {
        console.warn("[storage] user_platform_roles table missing; returning no extra roles");
        return [];
      }
      throw e;
    }
  }

  async addUserRole(userId: string, roleSlug: string, assignedBy?: string): Promise<void> {
    try {
      await db.insert(userPlatformRoles).values({
        id: crypto.randomUUID(),
        userId,
        roleSlug,
        assignedBy: assignedBy ?? null,
      }).onConflictDoNothing();
    } catch (e) {
      const msg = String((e as { message?: string })?.message ?? "");
      if (msg.includes("user_platform_roles") && msg.includes("does not exist")) {
        console.warn("[storage] user_platform_roles table missing; addUserRole skipped");
        return;
      }
      throw e;
    }
  }

  async removeUserRole(userId: string, roleSlug: string): Promise<void> {
    try {
      await db.delete(userPlatformRoles).where(
        and(eq(userPlatformRoles.userId, userId), eq(userPlatformRoles.roleSlug, roleSlug))
      );
    } catch (e) {
      const msg = String((e as { message?: string })?.message ?? "");
      if (msg.includes("user_platform_roles") && msg.includes("does not exist")) {
        console.warn("[storage] user_platform_roles table missing; removeUserRole skipped");
        return;
      }
      throw e;
    }
  }

  async setUserRoles(userId: string, slugs: string[], assignedBy?: string): Promise<void> {
    try {
      await db.delete(userPlatformRoles).where(eq(userPlatformRoles.userId, userId));
      if (slugs.length > 0) {
        await db.insert(userPlatformRoles).values(
          slugs.map((roleSlug) => ({ id: crypto.randomUUID(), userId, roleSlug, assignedBy: assignedBy ?? null }))
        );
      }
    } catch (e) {
      const msg = String((e as { message?: string })?.message ?? "");
      if (msg.includes("user_platform_roles") && msg.includes("does not exist")) {
        console.warn("[storage] user_platform_roles table missing; setUserRoles skipped");
        return;
      }
      throw e;
    }
  }

  async getLeavePolicyByBusinessId(businessId: string): Promise<LeavePolicy | null> {
    const [row] = await db.select().from(leavePolicies).where(eq(leavePolicies.businessId, businessId)).limit(1);
    return row ?? null;
  }

  async upsertLeavePolicy(businessId: string, data: Partial<InsertLeavePolicy>): Promise<LeavePolicy> {
    const existing = await this.getLeavePolicyByBusinessId(businessId);
    if (existing) {
      const [updated] = await db
        .update(leavePolicies)
        .set({ ...data, updatedAt: new Date() })
        .where(eq(leavePolicies.businessId, businessId))
        .returning();
      return updated;
    }
    const [created] = await db
      .insert(leavePolicies)
      .values({ id: crypto.randomUUID(), businessId, ...data })
      .returning();
    return created;
  }

  async getLeaveBalances(employeeErpId: string, leaveYear: number): Promise<LeaveBalance[]> {
    return db.select().from(leaveBalances).where(
      and(eq(leaveBalances.employeeErpId, employeeErpId), eq(leaveBalances.leaveYear, leaveYear))
    );
  }

  async getLeaveBalanceByUserId(userId: string, leaveType: string, leaveYear: number): Promise<LeaveBalance | null> {
    const [row] = await db.select().from(leaveBalances).where(
      and(
        eq(leaveBalances.userId, userId),
        eq(leaveBalances.leaveType, leaveType),
        eq(leaveBalances.leaveYear, leaveYear),
      )
    ).limit(1);
    return row ?? null;
  }

  async getLeaveBalance(employeeErpId: string, leaveType: string, leaveYear: number): Promise<LeaveBalance | null> {
    const [row] = await db.select().from(leaveBalances).where(
      and(
        eq(leaveBalances.employeeErpId, employeeErpId),
        eq(leaveBalances.leaveType, leaveType),
        eq(leaveBalances.leaveYear, leaveYear),
      )
    ).limit(1);
    return row ?? null;
  }

  async upsertLeaveBalance(data: Omit<InsertLeaveBalance, "id" | "createdAt" | "updatedAt">): Promise<LeaveBalance> {
    const existing = await this.getLeaveBalance(data.employeeErpId, data.leaveType, data.leaveYear);
    if (existing) {
      const [updated] = await db
        .update(leaveBalances)
        .set({ ...data, updatedAt: new Date() })
        .where(eq(leaveBalances.id, existing.id))
        .returning();
      return updated;
    }
    const [created] = await db
      .insert(leaveBalances)
      .values({ id: crypto.randomUUID(), ...data })
      .returning();
    return created;
  }

  async addLeaveBalanceUsed(employeeErpId: string, leaveType: string, leaveYear: number, days: number, forDate?: string): Promise<void> {
    const updated = await db.update(leaveBalances)
      .set({ usedDays: sql`used_days + ${days}`, updatedAt: new Date() })
      .where(
        and(
          eq(leaveBalances.employeeErpId, employeeErpId),
          eq(leaveBalances.leaveType, leaveType),
          eq(leaveBalances.leaveYear, leaveYear),
        )
      )
      .returning({ id: leaveBalances.id });
    // Fallback: if no calendar-year row matched, find the employee-year row covering forDate.
    if (updated.length === 0 && forDate) {
      const rows = await db.select().from(leaveBalances).where(
        and(
          eq(leaveBalances.employeeErpId, employeeErpId),
          eq(leaveBalances.leaveType, leaveType),
          isNotNull(leaveBalances.yearStartDate),
          lte(leaveBalances.yearStartDate, forDate),
        )
      ).orderBy(desc(leaveBalances.yearStartDate)).limit(1);
      if (rows.length > 0) {
        const row = rows[0];
        const yearEndMs = new Date(row.yearStartDate! + "T00:00:00").getTime() + 365 * 86_400_000;
        if (new Date(forDate + "T00:00:00").getTime() < yearEndMs) {
          await db.update(leaveBalances)
            .set({ usedDays: sql`used_days + ${days}`, updatedAt: new Date() })
            .where(eq(leaveBalances.id, row.id));
        }
      }
    }
  }

  async subtractLeaveBalanceUsed(employeeErpId: string, leaveType: string, leaveYear: number, days: number, forDate?: string): Promise<void> {
    const updated = await db.update(leaveBalances)
      .set({ usedDays: sql`GREATEST(0, used_days - ${days})`, updatedAt: new Date() })
      .where(
        and(
          eq(leaveBalances.employeeErpId, employeeErpId),
          eq(leaveBalances.leaveType, leaveType),
          eq(leaveBalances.leaveYear, leaveYear),
        )
      )
      .returning({ id: leaveBalances.id });
    // Fallback: if no calendar-year row matched, find the employee-year row covering forDate.
    if (updated.length === 0 && forDate) {
      const rows = await db.select().from(leaveBalances).where(
        and(
          eq(leaveBalances.employeeErpId, employeeErpId),
          eq(leaveBalances.leaveType, leaveType),
          isNotNull(leaveBalances.yearStartDate),
          lte(leaveBalances.yearStartDate, forDate),
        )
      ).orderBy(desc(leaveBalances.yearStartDate)).limit(1);
      if (rows.length > 0) {
        const row = rows[0];
        const yearEndMs = new Date(row.yearStartDate! + "T00:00:00").getTime() + 365 * 86_400_000;
        if (new Date(forDate + "T00:00:00").getTime() < yearEndMs) {
          await db.update(leaveBalances)
            .set({ usedDays: sql`GREATEST(0, used_days - ${days})`, updatedAt: new Date() })
            .where(eq(leaveBalances.id, row.id));
        }
      }
    }
  }

  async getLeaveBalancesByYearStart(employeeErpId: string, yearStartDate: string): Promise<LeaveBalance[]> {
    return db.select().from(leaveBalances).where(
      and(eq(leaveBalances.employeeErpId, employeeErpId), eq(leaveBalances.yearStartDate, yearStartDate))
    );
  }

  async getLeaveBalanceByYearStart(employeeErpId: string, leaveType: string, yearStartDate: string): Promise<LeaveBalance | null> {
    const [row] = await db.select().from(leaveBalances).where(
      and(
        eq(leaveBalances.employeeErpId, employeeErpId),
        eq(leaveBalances.leaveType, leaveType),
        eq(leaveBalances.yearStartDate, yearStartDate),
      )
    );
    return row ?? null;
  }

  async getAllAnnualLeaveBalancesWithYearStart(): Promise<LeaveBalance[]> {
    return db.select().from(leaveBalances).where(
      and(
        eq(leaveBalances.leaveType, "annual"),
        isNotNull(leaveBalances.yearStartDate),
      )
    );
  }

  async getAllAnnualLeaveBalancesWithCarryForward(): Promise<LeaveBalance[]> {
    return db.select().from(leaveBalances).where(
      and(
        eq(leaveBalances.leaveType, "annual"),
        isNotNull(leaveBalances.carriedForwardAt),
        sql`carried_forward_days > 0`,
      )
    );
  }

  async updateLeaveBalanceCarryForward(id: string, carriedForwardDays: number, carriedForwardAt: Date | null): Promise<void> {
    await db.update(leaveBalances).set({
      carriedForwardDays: String(carriedForwardDays),
      carriedForwardAt: carriedForwardAt ?? undefined,
      updatedAt: new Date(),
    }).where(eq(leaveBalances.id, id));
  }

  async createLeaveForfeitureLog(data: Omit<InsertLeaveForfeitureLog, "id" | "forfeitedAt">): Promise<LeaveForfeitureLog> {
    const [row] = await db.insert(leaveForfeitureLogs).values({
      id: crypto.randomUUID(),
      ...data,
    }).returning();
    return row;
  }

  async getLeaveForfeitureLogs(businessId: string, limit = 200): Promise<LeaveForfeitureLog[]> {
    return db.select().from(leaveForfeitureLogs)
      .where(eq(leaveForfeitureLogs.businessId, businessId))
      .orderBy(desc(leaveForfeitureLogs.forfeitedAt))
      .limit(limit);
  }

  async createLeaveWarningLog(data: Omit<InsertLeaveWarningLog, "id" | "sentAt">): Promise<LeaveWarningLog> {
    const [row] = await db.insert(leaveWarningLogs).values({
      id: crypto.randomUUID(),
      ...data,
    }).onConflictDoNothing().returning();
    return row as LeaveWarningLog;
  }

  async hasLeaveWarningBeenSent(employeeErpId: string, warningType: string, leaveYear: number): Promise<boolean> {
    const [row] = await db.select({ id: leaveWarningLogs.id }).from(leaveWarningLogs).where(
      and(
        eq(leaveWarningLogs.employeeErpId, employeeErpId),
        eq(leaveWarningLogs.warningType, warningType),
        eq(leaveWarningLogs.leaveYear, leaveYear),
      )
    ).limit(1);
    return !!row;
  }

  async createSalaryAdvanceApproval(row: {
    businessId: string;
    erpAdvanceName: string;
    employeeErpId: string | null;
    approverUserId: string;
    action: "approved" | "rejected";
    remarks: string | null;
  }): Promise<void> {
    await db.insert(salaryAdvanceApprovals).values({
      id: crypto.randomUUID(),
      businessId: row.businessId,
      erpAdvanceName: row.erpAdvanceName,
      employeeErpId: row.employeeErpId,
      approverUserId: row.approverUserId,
      action: row.action,
      remarks: row.remarks,
    });
  }

  async getSalaryAdvanceApprovalByErpName(
    businessId: string,
    erpAdvanceName: string,
  ): Promise<{
    action: string;
    remarks: string | null;
    createdAt: Date;
    approverName: string | null;
  } | null> {
    const rows = await db
      .select({
        action: salaryAdvanceApprovals.action,
        remarks: salaryAdvanceApprovals.remarks,
        createdAt: salaryAdvanceApprovals.createdAt,
        approverName: users.fullName,
      })
      .from(salaryAdvanceApprovals)
      .leftJoin(users, eq(salaryAdvanceApprovals.approverUserId, users.id))
      .where(
        and(
          eq(salaryAdvanceApprovals.businessId, businessId),
          eq(salaryAdvanceApprovals.erpAdvanceName, erpAdvanceName),
        ),
      )
      .orderBy(desc(salaryAdvanceApprovals.createdAt))
      .limit(1);
    const r = rows[0];
    if (!r) return null;
    return {
      action: r.action,
      remarks: r.remarks,
      createdAt: r.createdAt,
      approverName: r.approverName,
    };
  }

  async createLeaveApplication(data: Omit<InsertLeaveApplication, "id" | "createdAt" | "updatedAt">): Promise<LeaveApplication> {
    const [row] = await db.insert(leaveApplications)
      .values({ id: crypto.randomUUID(), ...data })
      .returning();
    return row;
  }

  async getLeaveApplication(id: string): Promise<LeaveApplication | null> {
    const [row] = await db.select().from(leaveApplications).where(eq(leaveApplications.id, id)).limit(1);
    return row ?? null;
  }

  async listLeaveApplications(businessId: string, opts: {
    page: number; pageSize: number; status?: string; employeeErpId?: string; userId?: string;
  }): Promise<{ rows: LeaveApplication[]; hasMore: boolean }> {
    const { page, pageSize, status, employeeErpId, userId } = opts;
    const conditions = [eq(leaveApplications.businessId, businessId)];
    if (status && status !== "all") conditions.push(eq(leaveApplications.status, status));
    if (employeeErpId) conditions.push(eq(leaveApplications.employeeErpId, employeeErpId));
    else if (userId) conditions.push(eq(leaveApplications.userId, userId));
    const offset = (page - 1) * pageSize;
    const rows = await db.select().from(leaveApplications)
      .where(and(...conditions))
      .orderBy(desc(leaveApplications.createdAt))
      .limit(pageSize + 1)
      .offset(offset);
    const hasMore = rows.length > pageSize;
    return { rows: rows.slice(0, pageSize), hasMore };
  }

  async updateLeaveApplicationStatus(id: string, status: string, extra?: {
    reviewedByUserId?: string; rejectionReason?: string; leaveApproverEmail?: string;
  }): Promise<LeaveApplication | null> {
    const patch: Record<string, unknown> = { status, updatedAt: new Date() };
    if (extra?.reviewedByUserId) { patch.reviewedByUserId = extra.reviewedByUserId; patch.reviewedAt = new Date(); }
    if (extra?.rejectionReason !== undefined) patch.rejectionReason = extra.rejectionReason;
    if (extra?.leaveApproverEmail !== undefined) patch.leaveApproverEmail = extra.leaveApproverEmail;
    const [row] = await db.update(leaveApplications).set(patch).where(eq(leaveApplications.id, id)).returning();
    return row ?? null;
  }

  async countPendingLeaveApplications(businessId: string, scope: {
    userId?: string; employeeErpId?: string;
  }): Promise<number> {
    const conditions = [eq(leaveApplications.businessId, businessId), eq(leaveApplications.status, "pending")];
    if (scope.employeeErpId) conditions.push(eq(leaveApplications.employeeErpId, scope.employeeErpId));
    else if (scope.userId) conditions.push(eq(leaveApplications.userId, scope.userId));
    const [{ count }] = await db.select({ count: sql<number>`count(*)::int` })
      .from(leaveApplications).where(and(...conditions));
    return count ?? 0;
  }

  async hasFuturePendingLeaveApplication(businessId: string, scope: { employeeErpId?: string; userId?: string }): Promise<boolean> {
    const today = new Date().toISOString().slice(0, 10);
    const conditions = [
      eq(leaveApplications.businessId, businessId),
      eq(leaveApplications.status, "pending"),
      gte(leaveApplications.fromDate, today),
    ];
    if (scope.employeeErpId) conditions.push(eq(leaveApplications.employeeErpId, scope.employeeErpId));
    else if (scope.userId) conditions.push(eq(leaveApplications.userId, scope.userId));
    const [{ count }] = await db.select({ count: sql<number>`count(*)::int` })
      .from(leaveApplications).where(and(...conditions));
    return (count ?? 0) > 0;
  }

  async getApprovedLeaveDays(businessId: string, scope: { employeeErpId?: string; userId?: string }, leaveType: string, yearStart: string, yearEnd: string): Promise<number> {
    const conditions = [
      eq(leaveApplications.businessId, businessId),
      eq(leaveApplications.leaveType, leaveType),
      eq(leaveApplications.status, "approved"),
      gte(leaveApplications.fromDate, yearStart),
      lte(leaveApplications.fromDate, yearEnd),
    ];
    if (scope.employeeErpId) conditions.push(eq(leaveApplications.employeeErpId, scope.employeeErpId));
    else if (scope.userId) conditions.push(eq(leaveApplications.userId, scope.userId));
    const [{ total }] = await db.select({ total: sql<string>`COALESCE(SUM(total_days), 0)` })
      .from(leaveApplications).where(and(...conditions));
    return parseFloat(total ?? "0");
  }

  async hasApprovedFullDayLeaveOnDate(employeeErpId: string, date: string): Promise<boolean> {
    const [{ count }] = await db.select({ count: sql<number>`count(*)::int` })
      .from(leaveApplications)
      .where(and(
        eq(leaveApplications.employeeErpId, employeeErpId),
        eq(leaveApplications.status, "approved"),
        eq(leaveApplications.halfDay, false),
        lte(leaveApplications.fromDate, date),
        gte(leaveApplications.toDate, date),
      ));
    return (count ?? 0) > 0;
  }

  async getOvertimePolicyByBusinessId(businessId: string): Promise<OvertimePolicy | null> {
    const [row] = await db.select().from(overtimePolicies).where(eq(overtimePolicies.businessId, businessId)).limit(1);
    return row ?? null;
  }

  async upsertOvertimePolicy(businessId: string, data: Partial<InsertOvertimePolicy>): Promise<OvertimePolicy> {
    const existing = await this.getOvertimePolicyByBusinessId(businessId);
    if (existing) {
      const [updated] = await db.update(overtimePolicies)
        .set({ ...data, updatedAt: new Date() })
        .where(eq(overtimePolicies.businessId, businessId))
        .returning();
      return updated;
    }
    const [created] = await db.insert(overtimePolicies)
      .values({ id: crypto.randomUUID(), businessId, ...data })
      .returning();
    return created;
  }

  async createCompulsoryLeave(data: Omit<InsertCompulsoryLeave, "id" | "createdAt">): Promise<CompulsoryLeave> {
    const [row] = await db.insert(compulsoryLeaves).values({ id: crypto.randomUUID(), ...data }).returning();
    return row;
  }

  async getCompulsoryLeavesByBusiness(businessId: string): Promise<CompulsoryLeave[]> {
    return db.select().from(compulsoryLeaves)
      .where(and(eq(compulsoryLeaves.businessId, businessId), isNull(compulsoryLeaves.revokedAt)))
      .orderBy(sql`${compulsoryLeaves.createdAt} desc`);
  }

  async getActiveCompulsoryLeaves(employeeErpId: string, date: string): Promise<CompulsoryLeave[]> {
    return db.select().from(compulsoryLeaves).where(
      and(
        eq(compulsoryLeaves.employeeErpId, employeeErpId),
        isNull(compulsoryLeaves.revokedAt),
        sql`${compulsoryLeaves.startDate} <= ${date}`,
        sql`${compulsoryLeaves.endDate} >= ${date}`,
      )
    );
  }

  async getActiveCompulsoryLeavesByUserId(userId: string, date: string): Promise<CompulsoryLeave[]> {
    return db.select().from(compulsoryLeaves).where(
      and(
        eq(compulsoryLeaves.blockedUserId, userId),
        isNull(compulsoryLeaves.revokedAt),
        sql`${compulsoryLeaves.startDate} <= ${date}`,
        sql`${compulsoryLeaves.endDate} >= ${date}`,
      )
    );
  }

  async getCompulsoryLeavesInRange(employeeErpId: string, from: string, to: string): Promise<CompulsoryLeave[]> {
    return db.select().from(compulsoryLeaves).where(
      and(
        eq(compulsoryLeaves.employeeErpId, employeeErpId),
        isNull(compulsoryLeaves.revokedAt),
        sql`${compulsoryLeaves.startDate} <= ${to}`,
        sql`${compulsoryLeaves.endDate} >= ${from}`,
      )
    );
  }

  async revokeCompulsoryLeave(id: string, revokedByUserId: string): Promise<CompulsoryLeave | null> {
    const [row] = await db.update(compulsoryLeaves)
      .set({ revokedAt: new Date(), revokedByUserId })
      .where(eq(compulsoryLeaves.id, id))
      .returning();
    return row ?? null;
  }

  async upsertEmployeeUserLink(userId: string, erpEmployeeId: string): Promise<void> {
    await db.insert(employeeUserLinks)
      .values({ userId, erpEmployeeId, updatedAt: new Date() })
      .onConflictDoUpdate({
        target: employeeUserLinks.userId,
        set: { erpEmployeeId, updatedAt: new Date() },
      });
  }

  async getEmployeeErpIdByUserId(userId: string): Promise<string | null> {
    // Primary: employeeUserLinks (auto-populated whenever the user logs in via BFF)
    const [link] = await db.select({ erpEmployeeId: employeeUserLinks.erpEmployeeId })
      .from(employeeUserLinks)
      .where(eq(employeeUserLinks.userId, userId))
      .limit(1);
    if (link?.erpEmployeeId) return link.erpEmployeeId;

    // Secondary: leaveBalances table (populated during provisioning)
    const [lb] = await db.select({ employeeErpId: leaveBalances.employeeErpId })
      .from(leaveBalances)
      .where(and(eq(leaveBalances.userId, userId), isNotNull(leaveBalances.employeeErpId)))
      .limit(1);
    if (lb?.employeeErpId) return lb.employeeErpId;

    // Fallback: hrEmployeeDrafts — set when an employee is created/completed via Pay Hub
    const [draft] = await db.select({ erpEmployeeName: hrEmployeeDrafts.erpEmployeeName })
      .from(hrEmployeeDrafts)
      .where(and(eq(hrEmployeeDrafts.userId, userId), isNotNull(hrEmployeeDrafts.erpEmployeeName)))
      .limit(1);
    return draft?.erpEmployeeName ?? null;
  }

  async getUserIdByEmployeeErpId(employeeErpId: string): Promise<string | null> {
    // Primary: employeeUserLinks (auto-populated whenever the user logs in via BFF)
    const [link] = await db.select({ userId: employeeUserLinks.userId })
      .from(employeeUserLinks)
      .where(eq(employeeUserLinks.erpEmployeeId, employeeErpId))
      .limit(1);
    if (link?.userId) return link.userId;

    // Secondary: leaveBalances table
    const [lb] = await db.select({ userId: leaveBalances.userId })
      .from(leaveBalances)
      .where(and(eq(leaveBalances.employeeErpId, employeeErpId), isNotNull(leaveBalances.userId)))
      .limit(1);
    if (lb?.userId) return lb.userId;

    // Fallback: hrEmployeeDrafts
    const [draft] = await db.select({ userId: hrEmployeeDrafts.userId })
      .from(hrEmployeeDrafts)
      .where(and(eq(hrEmployeeDrafts.erpEmployeeName, employeeErpId), isNotNull(hrEmployeeDrafts.userId)))
      .limit(1);
    return draft?.userId ?? null;
  }

  async createPaymentLink(
    link: InsertPaymentLink & { createdByUserId?: string | null; createdByName?: string | null },
  ): Promise<PaymentLink> {
    const [result] = await db
      .insert(paymentLinks)
      .values({ ...link, id: crypto.randomUUID() } as typeof paymentLinks.$inferInsert)
      .returning();
    return result;
  }

  async getPaymentLinks(userId?: string): Promise<PaymentLink[]> {
    if (userId) {
      return db.select().from(paymentLinks).where(eq(paymentLinks.createdByUserId, userId)).orderBy(desc(paymentLinks.createdAt));
    }
    return db.select().from(paymentLinks).orderBy(desc(paymentLinks.createdAt));
  }

  async getPaymentLinksByUserIds(userIds: string[]): Promise<PaymentLink[]> {
    if (!userIds.length) return [];
    return db.select().from(paymentLinks)
      .where(inArray(paymentLinks.createdByUserId, userIds))
      .orderBy(desc(paymentLinks.createdAt));
  }

  async getPaymentLinkById(id: string): Promise<PaymentLink | undefined> {
    const [result] = await db.select().from(paymentLinks).where(eq(paymentLinks.id, id));
    return result;
  }

  async getPaymentLinkByToken(token: string): Promise<PaymentLink | undefined> {
    const [result] = await db.select().from(paymentLinks).where(eq(paymentLinks.token, token));
    return result;
  }

  async getPaymentLinkByCheckoutRequestId(checkoutRequestId: string): Promise<PaymentLink | undefined> {
    const [result] = await db.select().from(paymentLinks).where(eq(paymentLinks.checkoutRequestId, checkoutRequestId));
    return result;
  }

  async updatePaymentLinkStatus(id: string, status: string): Promise<PaymentLink | undefined> {
    const updates: any = { status };
    if (status === "paid") updates.paidAt = new Date();
    const [result] = await db.update(paymentLinks).set(updates).where(eq(paymentLinks.id, id)).returning();
    return result;
  }

  async updatePaymentLinkByCheckoutRequestId(checkoutRequestId: string, data: Partial<PaymentLink>): Promise<PaymentLink | undefined> {
    const [result] = await db.update(paymentLinks).set(data).where(eq(paymentLinks.checkoutRequestId, checkoutRequestId)).returning();
    return result;
  }

  async updatePaymentLinkByToken(token: string, data: Partial<PaymentLink>): Promise<PaymentLink | undefined> {
    const [result] = await db.update(paymentLinks).set(data).where(eq(paymentLinks.token, token)).returning();
    return result;
  }

  async deletePaymentLink(id: string): Promise<void> {
    await db.delete(paymentLinks).where(eq(paymentLinks.id, id));
  }

  // ──────────── Wallet Top-ups ────────────
  async createWalletTopup(topup: InsertWalletTopup): Promise<WalletTopup> {
    const [result] = await db.insert(walletTopups).values({ ...topup, id: crypto.randomUUID() }).returning();
    return result;
  }

  async getWalletTopups(userId?: string): Promise<WalletTopup[]> {
    if (userId) {
      const actor = await this.getUserById(userId);
      const ownedBusiness = await this.getBusinessByUserId(userId);
      const resolvedBusinessId = actor?.businessId || ownedBusiness?.id || null;
      let businessOwnerUserId = ownedBusiness?.userId || null;
      // Member users don't own the business — resolve owner from the business record
      if (resolvedBusinessId && !businessOwnerUserId) {
        const biz = await this.getBusinessById(resolvedBusinessId);
        businessOwnerUserId = biz?.userId || null;
      }

      if (resolvedBusinessId) {
        return db
          .select()
          .from(walletTopups)
          .where(
            sql`(
              ${walletTopups.userId} IN (SELECT ${users.id} FROM ${users} WHERE ${users.businessId} = ${resolvedBusinessId})
              ${businessOwnerUserId ? sql` OR ${walletTopups.userId} = ${businessOwnerUserId}` : sql``}
            )`,
          )
          .orderBy(desc(walletTopups.createdAt));
      }
      return db.select().from(walletTopups).where(eq(walletTopups.userId, userId)).orderBy(desc(walletTopups.createdAt));
    }
    return db.select().from(walletTopups).orderBy(desc(walletTopups.createdAt));
  }

  async getWalletTopupsByUserIds(userIds: string[]): Promise<WalletTopup[]> {
    if (!userIds.length) return [];
    return db.select().from(walletTopups)
      .where(inArray(walletTopups.userId, userIds))
      .orderBy(desc(walletTopups.createdAt));
  }

  async getWalletTopupById(id: string): Promise<WalletTopup | undefined> {
    const [result] = await db.select().from(walletTopups).where(eq(walletTopups.id, id));
    return result;
  }

  async getWalletTopupByCheckoutRequestId(checkoutRequestId: string): Promise<WalletTopup | undefined> {
    const [result] = await db.select().from(walletTopups).where(eq(walletTopups.checkoutRequestId, checkoutRequestId));
    return result;
  }

  async getWalletTopupByMpesaReceiptNumber(receipt: string): Promise<WalletTopup | undefined> {
    const t = String(receipt ?? "").trim();
    if (!t) return undefined;
    const [result] = await db
      .select()
      .from(walletTopups)
      .where(or(eq(walletTopups.reference, t), eq(walletTopups.mpesaReceiptNumber, t)))
      .limit(1);
    return result;
  }

  async updateWalletTopupByCheckoutRequestId(checkoutRequestId: string, data: Partial<InsertWalletTopup>): Promise<WalletTopup | undefined> {
    const [result] = await db.update(walletTopups).set(data).where(eq(walletTopups.checkoutRequestId, checkoutRequestId)).returning();
    return result;
  }

  async updateWalletTopupStatus(id: string, status: "approved" | "rejected", extra?: { mpesaReceiptNumber?: string; checkoutResultCode?: string; checkoutResultDesc?: string; callbackPayload?: string }): Promise<WalletTopup | undefined> {
    const [result] = await db.update(walletTopups)
      .set({
        status,
        ...(extra?.mpesaReceiptNumber !== undefined ? { mpesaReceiptNumber: extra.mpesaReceiptNumber } : {}),
        ...(extra?.checkoutResultCode !== undefined ? { checkoutResultCode: extra.checkoutResultCode } : {}),
        ...(extra?.checkoutResultDesc !== undefined ? { checkoutResultDesc: extra.checkoutResultDesc } : {}),
        ...(extra?.callbackPayload !== undefined ? { callbackPayload: extra.callbackPayload } : {}),
      })
      // Only update if still pending — prevents double-credit if callback arrives simultaneously
      .where(and(eq(walletTopups.id, id), eq(walletTopups.status, "pending_review")))
      .returning();
    return result;
  }

  async reviewWalletTopup(id: string, reviewedBy: string, reviewerName: string, action: "approved" | "rejected", note?: string): Promise<WalletTopup | undefined> {
    const [result] = await db.update(walletTopups)
      .set({
        status: action,
        reviewedBy,
        reviewerName,
        reviewNote: note || null,
        reviewedAt: new Date(),
      })
      .where(eq(walletTopups.id, id))
      .returning();
    return result;
  }

  async getPaymentSettingsByBusinessId(businessId: string): Promise<BusinessPaymentSettings | undefined> {
    const [row] = await db.select().from(businessPaymentSettings).where(eq(businessPaymentSettings.businessId, businessId));
    return row;
  }

  async upsertPaymentSettings(businessId: string, data: Partial<InsertBusinessPaymentSettings>): Promise<BusinessPaymentSettings> {
    const now = new Date();
    const [existing] = await db.select().from(businessPaymentSettings).where(eq(businessPaymentSettings.businessId, businessId));
    if (existing) {
      const setObj: Record<string, unknown> = { updatedAt: now };
      if (data.feePolicyMode !== undefined) setObj.feePolicyMode = data.feePolicyMode;
      if (data.feeShareSenderPercent !== undefined) setObj.feeShareSenderPercent = data.feeShareSenderPercent;
      if (data.feeShareReceiverPercent !== undefined) setObj.feeShareReceiverPercent = data.feeShareReceiverPercent;
      if (data.otpRequiredBulk !== undefined) setObj.otpRequiredBulk = data.otpRequiredBulk;
      if (data.otpRequiredSingle !== undefined) setObj.otpRequiredSingle = data.otpRequiredSingle;
      if (data.notifyEmail !== undefined) setObj.notifyEmail = data.notifyEmail;
      if (data.notifySms !== undefined) setObj.notifySms = data.notifySms;
      if (data.notifyWhatsapp !== undefined) setObj.notifyWhatsapp = data.notifyWhatsapp;
      const [updated] = await db.update(businessPaymentSettings).set(setObj as any).where(eq(businessPaymentSettings.businessId, businessId)).returning();
      return updated;
    }
    const [created] = await db.insert(businessPaymentSettings).values({
      id: crypto.randomUUID(),
      businessId,
      feePolicyMode: data.feePolicyMode ?? "SENDER",
      feeShareSenderPercent: data.feeShareSenderPercent ?? 100,
      feeShareReceiverPercent: data.feeShareReceiverPercent ?? 0,
      otpRequiredBulk: data.otpRequiredBulk ?? false,
      otpRequiredSingle: data.otpRequiredSingle ?? false,
      notifyEmail: data.notifyEmail ?? true,
      notifySms: data.notifySms ?? false,
      notifyWhatsapp: data.notifyWhatsapp ?? false,
      createdAt: now,
      updatedAt: now,
    }).returning();
    return created;
  }

  async getCompanyCustomRates(businessId: string): Promise<{ useCustomRates: boolean; customFeeSchedule: Array<{ min: number; max: number; fee: number }> | null; customMaxAmount: number | null }> {
    const [row] = await db.select({
      useCustomRates: businessPaymentSettings.useCustomRates,
      customFeeSchedule: businessPaymentSettings.customFeeSchedule,
      customMaxAmount: businessPaymentSettings.customMaxAmount,
    }).from(businessPaymentSettings).where(eq(businessPaymentSettings.businessId, businessId));
    if (!row) return { useCustomRates: false, customFeeSchedule: null, customMaxAmount: null };
    return {
      useCustomRates: row.useCustomRates,
      customFeeSchedule: row.customFeeSchedule as Array<{ min: number; max: number; fee: number }> | null,
      customMaxAmount: row.customMaxAmount ?? null,
    };
  }

  async upsertCompanyCustomRates(businessId: string, useCustomRates: boolean, customFeeSchedule: Array<{ min: number; max: number; fee: number }> | null, customMaxAmount: number | null): Promise<void> {
    const now = new Date();
    const [existing] = await db.select({ id: businessPaymentSettings.id }).from(businessPaymentSettings).where(eq(businessPaymentSettings.businessId, businessId));
    if (existing) {
      await db.update(businessPaymentSettings)
        .set({ useCustomRates, customFeeSchedule: customFeeSchedule as any, customMaxAmount, updatedAt: now })
        .where(eq(businessPaymentSettings.businessId, businessId));
    } else {
      await db.insert(businessPaymentSettings).values({
        id: crypto.randomUUID(),
        businessId,
        useCustomRates,
        customFeeSchedule: customFeeSchedule as any,
        customMaxAmount,
        createdAt: now,
        updatedAt: now,
      });
    }
  }

  async getPaymentFavoritesByBusinessId(businessId: string): Promise<PaymentFavorite[]> {
    return db.select().from(paymentFavorites).where(eq(paymentFavorites.businessId, businessId)).orderBy(desc(paymentFavorites.createdAt));
  }

  async createPaymentFavorite(favorite: InsertPaymentFavorite): Promise<PaymentFavorite> {
    const [result] = await db.insert(paymentFavorites).values({ ...favorite, id: crypto.randomUUID(), createdAt: new Date(), updatedAt: new Date() }).returning();
    return result;
  }

  async updatePaymentFavorite(id: string, businessId: string, data: Partial<Pick<PaymentFavorite, "name" | "data" | "isActive">>): Promise<PaymentFavorite | undefined> {
    const [result] = await db.update(paymentFavorites).set({ ...data, updatedAt: new Date() }).where(and(eq(paymentFavorites.id, id), eq(paymentFavorites.businessId, businessId))).returning();
    return result;
  }

  async deletePaymentFavorite(id: string, businessId: string): Promise<void> {
    await db.delete(paymentFavorites).where(and(eq(paymentFavorites.id, id), eq(paymentFavorites.businessId, businessId)));
  }

  // ──────────── Quick Pay Recipients ────────────
  async enableTwoFactor(userId: string, secret: string): Promise<void> {
    await db.update(users).set({ twoFactorSecret: secret, twoFactorEnabled: true }).where(eq(users.id, userId));
  }

  async disableTwoFactor(userId: string): Promise<void> {
    await db.update(users).set({ twoFactorSecret: null, twoFactorEnabled: false }).where(eq(users.id, userId));
  }

  async setUser2faMethod(userId: string, method: "email_otp" | "totp" | null): Promise<void> {
    const updates: Record<string, unknown> = { login2faMethod: method };
    if (method !== "totp") {
      // clear TOTP secret when switching away from authenticator
      updates.twoFactorSecret = null;
      updates.twoFactorEnabled = false;
    }
    await db.update(users).set(updates as any).where(eq(users.id, userId));
  }

  async setCompany2fa(businessId: string, enforced: boolean, method: string | null): Promise<void> {
    await db.update(businesses).set({ company2faEnforced: enforced, company2faMethod: method }).where(eq(businesses.id, businessId));
  }

  async createQuickPayRecipient(recipient: InsertQuickPayRecipient): Promise<QuickPayRecipient> {
    const [result] = await db.insert(quickPayRecipients).values({ ...recipient, id: crypto.randomUUID() }).returning();
    return result;
  }

  async getQuickPayRecipients(userId: string, _businessId?: string | null): Promise<QuickPayRecipient[]> {
    // Personal favorites only — never expose other users' recipients (tenant-wide lists are not shared here).
    return db.select().from(quickPayRecipients)
      .where(eq(quickPayRecipients.userId, userId))
      .orderBy(desc(quickPayRecipients.createdAt));
  }

  async getQuickPayRecipientById(id: string): Promise<QuickPayRecipient | undefined> {
    const [result] = await db.select().from(quickPayRecipients).where(eq(quickPayRecipients.id, id));
    return result;
  }

  async deleteQuickPayRecipient(id: string, userId: string, _businessId?: string | null): Promise<void> {
    await db.delete(quickPayRecipients).where(
      and(eq(quickPayRecipients.id, id), eq(quickPayRecipients.userId, userId)),
    );
  }

  async createMpesaAuditLog(type: string, payload: unknown): Promise<MpesaAuditLog> {
    const [result] = await db.insert(mpesaAuditLogs).values({
      id: crypto.randomUUID(),
      type,
      payload: JSON.stringify(payload),
    }).returning();
    return result;
  }

  async getMpesaAuditLogs(limit = 50): Promise<MpesaAuditLog[]> {
    return db.select().from(mpesaAuditLogs).orderBy(desc(mpesaAuditLogs.createdAt)).limit(limit);
  }

  async getMpesaAuditLogsByTypes(types: string[], limit = 500): Promise<MpesaAuditLog[]> {
    return db.select().from(mpesaAuditLogs)
      .where(inArray(mpesaAuditLogs.type, types))
      .orderBy(desc(mpesaAuditLogs.createdAt))
      .limit(limit);
  }

  async getLatestMpesaAuditLogMatchingPayload(
    type: string,
    payloadSubstring: string,
  ): Promise<MpesaAuditLog | undefined> {
    const needle = `%${payloadSubstring}%`;
    const [row] = await db
      .select()
      .from(mpesaAuditLogs)
      .where(and(eq(mpesaAuditLogs.type, type), sql`${mpesaAuditLogs.payload}::text LIKE ${needle}`))
      .orderBy(desc(mpesaAuditLogs.createdAt))
      .limit(1);
    return row;
  }

  async createHakikishaLookup(phone: string, originatorConversationId: string): Promise<HakikishaLookup> {
    const [row] = await db.insert(hakikishaLookups).values({
      phone,
      originatorConversationId,
      status: "pending",
    }).returning();
    if (!row) throw new Error("Failed to create hakikisha lookup");
    return row;
  }

  async updateHakikishaLookupByOriginatorId(
    originatorConversationId: string,
    data: {
      registeredName?: string;
      transactionId?: string;
      status: string;
      mpesaResultCode?: string | null;
      mpesaResultDesc?: string | null;
    },
  ): Promise<number> {
    const result = await db.update(hakikishaLookups).set({
      registeredName: data.registeredName,
      transactionId: data.transactionId,
      status: data.status,
      resolvedAt: new Date(),
      ...(data.mpesaResultCode !== undefined ? { mpesaResultCode: data.mpesaResultCode } : {}),
      ...(data.mpesaResultDesc !== undefined ? { mpesaResultDesc: data.mpesaResultDesc } : {}),
    })
      .where(eq(hakikishaLookups.originatorConversationId, originatorConversationId));
    return result.rowCount ?? 0;
  }

  async getHakikishaLookupByOriginatorId(originatorConversationId: string): Promise<HakikishaLookup | undefined> {
    const [row] = await db.select().from(hakikishaLookups)
      .where(eq(hakikishaLookups.originatorConversationId, originatorConversationId))
      .limit(1);
    return row;
  }

  async getHakikishaVerifiedNameByPhone(normalizedPhone254: string): Promise<string | null> {
    const [row] = await db
      .select({ registeredName: hakikishaLookups.registeredName })
      .from(hakikishaLookups)
      .where(
        and(
          eq(hakikishaLookups.phone, normalizedPhone254),
          eq(hakikishaLookups.status, "verified"),
          sql`${hakikishaLookups.registeredName} IS NOT NULL AND ${hakikishaLookups.registeredName} != ''`,
        ),
      )
      .orderBy(desc(hakikishaLookups.resolvedAt))
      .limit(1);
    return row?.registeredName ?? null;
  }

  /**
   * Bulk cache lookup — returns a map of normalized-phone → registered-name
   * for every phone that already has a verified Hakikisha result OR a recorded
   * M-Pesa name from a previous payment.  Single DB round-trip per source.
   */
  async getBulkHakikishaCachedNames(
    businessId: string | null,
    phones: string[],
  ): Promise<Record<string, string>> {
    if (phones.length === 0) return {};
    const result: Record<string, string> = {};

    // 1) Hakikisha lookup table (verified results)
    const hRows = await db
      .select({ phone: hakikishaLookups.phone, name: hakikishaLookups.registeredName })
      .from(hakikishaLookups)
      .where(
        and(
          inArray(hakikishaLookups.phone, phones),
          eq(hakikishaLookups.status, "verified"),
          sql`${hakikishaLookups.registeredName} IS NOT NULL AND ${hakikishaLookups.registeredName} != ''`,
        ),
      )
      .orderBy(desc(hakikishaLookups.resolvedAt));
    for (const row of hRows) {
      if (row.name && !result[row.phone]) result[row.phone] = row.name;
    }

    // 2) Previous payment names (business-scoped)
    if (businessId) {
      const tenDigits = phones
        .filter(p => p.length === 12 && p.startsWith("254"))
        .map(p => "0" + p.slice(3));
      const allVariants = [...phones, ...tenDigits];
      const batchCond = sql`(
        ${bulkPaymentBatches.createdByUserId} = (SELECT user_id FROM businesses WHERE id = ${businessId})
        OR ${bulkPaymentBatches.createdByUserId} IN (SELECT id FROM users WHERE business_id = ${businessId})
        OR ${bulkPaymentBatches.createdByUserId} IN (
          SELECT id FROM users
          WHERE created_by = (SELECT user_id FROM businesses WHERE id = ${businessId})
            AND business_id IS NULL
        )
      )`;
      const pRows = await db
        .select({ recipient: bulkPaymentItems.recipient, name: bulkPaymentItems.recipientNameFromMpesa })
        .from(bulkPaymentItems)
        .innerJoin(bulkPaymentBatches, eq(bulkPaymentItems.batchId, bulkPaymentBatches.id))
        .where(
          and(
            inArray(bulkPaymentItems.recipient, allVariants),
            eq(bulkPaymentBatches.paymentType, "mobile_money"),
            eq(bulkPaymentItems.status, "completed"),
            sql`${bulkPaymentItems.processedAt} IS NOT NULL`,
            sql`${bulkPaymentItems.recipientNameFromMpesa} IS NOT NULL AND ${bulkPaymentItems.recipientNameFromMpesa} != ''`,
            batchCond,
          ),
        )
        .orderBy(desc(bulkPaymentItems.processedAt));
      for (const row of pRows) {
        if (!row.name) continue;
        const cleanedName = row.name.includes(" - ")
          ? row.name.split(" - ").slice(1).join(" - ").trim()
          : row.name.trim();
        if (!cleanedName) continue;
        // normalize to 254 format so caller can match by the same key
        const phone254 =
          row.recipient.startsWith("0") && row.recipient.length === 10
            ? "254" + row.recipient.slice(1)
            : row.recipient;
        if (!result[phone254]) result[phone254] = cleanedName;
      }
    }

    return result;
  }

  async getPendingHakikishaLookupByPhone(normalizedPhone254: string): Promise<{ originatorConversationId: string } | null> {
    const tenMinutesAgo = new Date(Date.now() - 10 * 60 * 1000);
    const [row] = await db
      .select({ originatorConversationId: hakikishaLookups.originatorConversationId })
      .from(hakikishaLookups)
      .where(
        and(
          eq(hakikishaLookups.phone, normalizedPhone254),
          eq(hakikishaLookups.status, "pending"),
          gte(hakikishaLookups.createdAt, tenMinutesAgo),
        ),
      )
      .orderBy(desc(hakikishaLookups.createdAt))
      .limit(1);
    const id = row?.originatorConversationId;
    return id ? { originatorConversationId: id } : null;
  }

  async getHakikishaLookupsAdmin(opts: {
    page: number;
    limit: number;
    search?: string;
    status?: string;
  }): Promise<{ data: HakikishaLookup[]; total: number }> {
    const { page, limit, search, status } = opts;
    const offset = (page - 1) * limit;

    const conditions: SQL[] = [];
    if (status && status !== "all") conditions.push(eq(hakikishaLookups.status, status));
    if (search) {
      const q = `%${search.toLowerCase()}%`;
      conditions.push(sql`(lower(${hakikishaLookups.phone}) LIKE ${q} OR lower(coalesce(${hakikishaLookups.registeredName}, '')) LIKE ${q} OR lower(coalesce(${hakikishaLookups.transactionId}, '')) LIKE ${q})`);
    }

    const where = conditions.length ? and(...conditions) : undefined;

    const [rows, countRows] = await Promise.all([
      db.select().from(hakikishaLookups)
        .where(where)
        .orderBy(desc(hakikishaLookups.createdAt))
        .limit(limit)
        .offset(offset),
      db.select({ count: sql<number>`count(*)::int` }).from(hakikishaLookups).where(where),
    ]);

    return { data: rows, total: countRows[0]?.count ?? 0 };
  }

  async getCompanyWallets(opts: {
    page: number;
    limit: number;
    search?: string;
  }): Promise<{ data: CompanyWalletRow[]; total: number }> {
    const { page, limit, search } = opts;
    const offset = (page - 1) * limit;
    const searchParam = search ? `%${search.toLowerCase()}%` : null;

    // CTE-based query: compute per-company wallet balance in one pass
    const dataQuery = `
      WITH company_actors AS (
        SELECT b.id AS business_id, u.id AS user_id
        FROM businesses b
        JOIN users u ON u.business_id = b.id
        UNION
        SELECT b.id, b.user_id FROM businesses b
      ),
      topup_totals AS (
        SELECT ca.business_id, COALESCE(SUM(wt.amount::numeric), 0) AS total
        FROM company_actors ca
        JOIN wallet_topups wt ON wt.user_id = ca.user_id AND wt.status = 'approved'
        GROUP BY ca.business_id
      ),
      link_totals AS (
        SELECT ca.business_id, COALESCE(SUM(COALESCE(pl.paid_amount::numeric, pl.amount::numeric)), 0) AS total
        FROM company_actors ca
        JOIN payment_links pl ON pl.created_by_user_id = ca.user_id AND pl.status = 'paid'
        GROUP BY ca.business_id
      ),
      disbursement_totals AS (
        SELECT ca.business_id,
          COALESCE(SUM(CASE WHEN bpi.status = 'completed' THEN bpi.amount::numeric + bpi.fee::numeric ELSE 0 END), 0) AS completed,
          COALESCE(SUM(CASE WHEN bpi.status IN ('pending', 'processing') THEN bpi.amount::numeric + bpi.fee::numeric ELSE 0 END), 0) AS pending
        FROM company_actors ca
        JOIN bulk_payment_batches bpb ON bpb.created_by_user_id = ca.user_id
        JOIN bulk_payment_items bpi ON bpi.batch_id = bpb.id
        GROUP BY ca.business_id
      )
      SELECT
        b.id AS business_id,
        b.business_name,
        b.kyc_status,
        b.account_number,
        b.is_sandbox,
        b.created_at,
        COALESCE(tt.total, 0)::float AS total_topups,
        COALESCE(lt.total, 0)::float AS total_link_inflows,
        COALESCE(dt.completed, 0)::float AS total_disbursements,
        COALESCE(dt.pending, 0)::float AS pending_disbursements,
        (COALESCE(tt.total, 0) + COALESCE(lt.total, 0) - COALESCE(dt.completed, 0))::float AS current_balance
      FROM businesses b
      LEFT JOIN topup_totals tt ON tt.business_id = b.id
      LEFT JOIN link_totals lt ON lt.business_id = b.id
      LEFT JOIN disbursement_totals dt ON dt.business_id = b.id
      ${searchParam ? "WHERE lower(b.business_name) LIKE $3" : ""}
      ORDER BY current_balance DESC
      LIMIT $1 OFFSET $2
    `;

    const countQuery = `
      SELECT COUNT(*)::int AS total FROM businesses b
      ${searchParam ? "WHERE lower(b.business_name) LIKE $1" : ""}
    `;

    const dataParams = searchParam ? [limit, offset, searchParam] : [limit, offset];
    const countParams = searchParam ? [searchParam] : [];

    const [dataResult, countResult] = await Promise.all([
      pool.query<{
        business_id: string; business_name: string; kyc_status: string;
        account_number: string | null; is_sandbox: boolean; created_at: Date;
        total_topups: number; total_link_inflows: number; total_disbursements: number;
        pending_disbursements: number; current_balance: number;
      }>(dataQuery, dataParams),
      pool.query<{ total: number }>(countQuery, countParams),
    ]);

    const data: CompanyWalletRow[] = dataResult.rows.map((r) => ({
      businessId: r.business_id,
      businessName: r.business_name,
      kycStatus: r.kyc_status,
      accountNumber: r.account_number,
      isSandbox: r.is_sandbox,
      createdAt: r.created_at,
      totalTopups: Number(r.total_topups) || 0,
      totalLinkInflows: Number(r.total_link_inflows) || 0,
      totalDisbursements: Number(r.total_disbursements) || 0,
      pendingDisbursements: Number(r.pending_disbursements) || 0,
      currentBalance: Number(r.current_balance) || 0,
    }));

    return { data, total: countResult.rows[0]?.total ?? 0 };
  }

  async syncMpesaTransactions(): Promise<{ synced: number; updated: number; total: number }> {
    // Get all already-synced itemIds
    const existingItemIds = await db.select({ itemId: mpesaTransactions.itemId })
      .from(mpesaTransactions)
      .where(sql`${mpesaTransactions.itemId} IS NOT NULL`);
    const syncedSet = new Set(existingItemIds.map((r) => r.itemId).filter(Boolean));

    // Records that need re-evaluation: missing receipt/name OR still showing "unverified"
    const incompleteItems = await db.select({ itemId: mpesaTransactions.itemId })
      .from(mpesaTransactions)
      .where(and(
        sql`${mpesaTransactions.itemId} IS NOT NULL`,
        or(
          sql`${mpesaTransactions.mpesaTransactionId} IS NULL`,
          sql`${mpesaTransactions.recipientName} IS NULL`,
          eq(mpesaTransactions.status, "unverified"),
        )
      ));
    const incompleteSet = new Set(incompleteItems.map((r) => r.itemId).filter(Boolean));

    // Include ALL items that reached the M-Pesa API:
    //   - pending / processing / completed always qualify
    //   - failed qualifies ONLY if NOT an API-level HTTP rejection
    //     API rejections (400.002.02 etc.) have "errorCode" in the Safaricom response body —
    //     they never reached M-Pesa processing so are irrelevant for reconciliation
    const items = await db
      .select({
        itemId: bulkPaymentItems.id,
        batchId: bulkPaymentBatches.id,
        paymentType: bulkPaymentBatches.paymentType,
        recipient: bulkPaymentItems.recipient,
        accountNumber: bulkPaymentItems.accountNumber,
        amount: bulkPaymentItems.amount,
        fee: bulkPaymentItems.fee,
        reference: bulkPaymentItems.reference,
        status: bulkPaymentItems.status,
        failureReason: bulkPaymentItems.failureReason,
        batchName: bulkPaymentBatches.createdByName,
        batchCreatedAt: bulkPaymentBatches.createdAt,
        processedAt: bulkPaymentItems.processedAt,
      })
      .from(bulkPaymentItems)
      .innerJoin(bulkPaymentBatches, eq(bulkPaymentItems.batchId, bulkPaymentBatches.id))
      .where(
        or(
          inArray(bulkPaymentItems.status, ["pending", "processing", "completed"]),
          and(
            eq(bulkPaymentItems.status, "failed"),
            sql`(${bulkPaymentItems.failureReason} IS NULL OR ${bulkPaymentItems.failureReason} NOT LIKE '%"errorCode"%')`
          )
        )
      );

    // Build audit map from M-Pesa callbacks: itemId → { receipt, name, resultCode, processedAt }
    // resultCode=0 means M-Pesa confirmed success; >0 means M-Pesa rejected it
    const resultLogs = await db.select()
      .from(mpesaAuditLogs)
      .where(inArray(mpesaAuditLogs.type, ["b2c_result", "b2b_result"]))
      .orderBy(desc(mpesaAuditLogs.createdAt))
      .limit(5000);

    const auditMap = new Map<string, { mpesaTransactionId: string; recipientName: string; processedAt: Date; resultCode: number }>();
    for (const log of resultLogs) {
      try {
        const body = JSON.parse(log.payload);
        const result = body?.Result || body?.body?.Result || {};
        const transactionId = String(result?.TransactionID || "");
        const resultCode = Number(result?.ResultCode ?? -1);
        const params = result?.ResultParameters?.ResultParameter || [];
        const nameEntry = params.find((p: any) => p?.Key === "ReceiverPartyPublicName");
        const recipientName = String(nameEntry?.Value || "");
        const occasionEntry = String(params.find((p: any) => p?.Key === "Occasion")?.Value || "");

        let itemId = "";
        if (occasionEntry.includes("ITEM:")) {
          const [, itemPart] = occasionEntry.split("|");
          itemId = (itemPart || "").replace("ITEM:", "").trim();
        }
        if (itemId && !auditMap.has(itemId)) {
          auditMap.set(itemId, { mpesaTransactionId: transactionId, recipientName, processedAt: new Date(log.createdAt), resultCode });
        }
      } catch {
        // skip malformed logs
      }
    }

    let synced = 0;
    let updated = 0;

    for (const item of items) {
      const isNew = !syncedSet.has(item.itemId);
      const needsUpdate = incompleteSet.has(item.itemId);
      if (!isNew && !needsUpdate) continue;

      const audit = auditMap.get(item.itemId);

      // Derive statement status — reflects M-Pesa truth, not just our internal DB status:
      //   completed   → M-Pesa confirmed and our system recorded it correctly
      //   discrepancy → M-Pesa callback ResultCode=0 (success) but our system shows pending/failed
      //   failed      → M-Pesa callback ResultCode>0 (M-Pesa rejected it) OR our system recorded failure
      //   unverified  → request sent to M-Pesa but no callback received yet (network/endpoint issue)
      let statementStatus: string;
      if (item.status === "completed") {
        statementStatus = "completed";
      } else if (audit && audit.resultCode === 0) {
        statementStatus = "discrepancy";
      } else if ((audit && audit.resultCode > 0) || item.status === "failed") {
        statementStatus = "failed";
      } else {
        statementStatus = "unverified";
      }

      const failureReason =
        statementStatus === "discrepancy"
          ? "M-Pesa confirmed success (ResultCode=0) but our system shows this payment as pending/failed. Manual reconciliation required."
          : item.failureReason;

      const record = {
        itemId: item.itemId,
        batchId: item.batchId,
        paymentType: item.paymentType,
        recipient: item.recipient,
        accountNumber: item.accountNumber,
        amount: String(item.amount),
        fee: String(item.fee),
        reference: item.reference,
        mpesaTransactionId: audit?.mpesaTransactionId || null,
        recipientName: audit?.recipientName || null,
        status: statementStatus,
        failureReason,
        batchName: item.batchName,
        initiatedBy: null,
        processedAt: audit?.processedAt ?? item.processedAt ?? item.batchCreatedAt,
        syncedAt: new Date(),
      };

      if (isNew) {
        await db.insert(mpesaTransactions).values({ ...record, id: crypto.randomUUID() })
          .onConflictDoNothing();
        synced++;
      } else {
        await db.update(mpesaTransactions)
          .set({
            mpesaTransactionId: record.mpesaTransactionId,
            recipientName: record.recipientName,
            status: record.status,
            failureReason: record.failureReason,
            processedAt: record.processedAt,
            syncedAt: new Date(),
          })
          .where(eq(mpesaTransactions.itemId, item.itemId));
        updated++;
      }
    }

    const [{ count }] = await db.select({ count: sql<number>`COUNT(*)::int` }).from(mpesaTransactions);
    return { synced, updated, total: count };
  }

  async getMpesaTransactionById(id: string): Promise<MpesaTransaction | undefined> {
    const [result] = await db.select().from(mpesaTransactions).where(eq(mpesaTransactions.id, id));
    return result;
  }

  async updateMpesaTransactionStatus(
    id: string,
    data: { status: string; mpesaTransactionId?: string; recipientName?: string; processedAt?: Date; failureReason?: string }
  ): Promise<void> {
    const updates: Record<string, unknown> = { status: data.status, syncedAt: new Date() };
    if (data.mpesaTransactionId !== undefined) updates.mpesaTransactionId = data.mpesaTransactionId;
    if (data.recipientName !== undefined) updates.recipientName = data.recipientName;
    if (data.processedAt !== undefined) updates.processedAt = data.processedAt;
    if (data.failureReason !== undefined) updates.failureReason = data.failureReason;
    await db.update(mpesaTransactions).set(updates as any).where(eq(mpesaTransactions.id, id));
  }

  async findB2CRequestByItemId(itemId: string): Promise<{ conversationId: string; originatorConversationId: string } | null> {
    // b2c_request audit logs store { payload: <request body>, response: <Safaricom sync response> }
    // The request body contains the Occasion field: "BATCH:{batchId}|ITEM:{itemId}"
    const [log] = await db.select()
      .from(mpesaAuditLogs)
      .where(and(
        eq(mpesaAuditLogs.type, "b2c_request"),
        sql`${mpesaAuditLogs.payload}::text LIKE ${"%" + itemId + "%"}`
      ))
      .limit(1);

    if (!log) return null;
    try {
      const body = JSON.parse(log.payload);
      // The sync response from Safaricom is stored under body.response
      const resp = body?.response || {};
      return {
        conversationId: String(resp?.ConversationID || ""),
        originatorConversationId: String(resp?.OriginatorConversationID || ""),
      };
    } catch {
      return null;
    }
  }

  async findB2BRequestByItemId(itemId: string): Promise<{ conversationId: string; originatorConversationId: string } | null> {
    const [log] = await db
      .select()
      .from(mpesaAuditLogs)
      .where(
        and(
          eq(mpesaAuditLogs.type, "b2b_request"),
          sql`${mpesaAuditLogs.payload}::text LIKE ${"%" + itemId + "%"}`,
        ),
      )
      .limit(1);

    if (!log) return null;
    try {
      const body = JSON.parse(log.payload);
      const resp = body?.response || {};
      return {
        conversationId: String(resp?.ConversationID || ""),
        originatorConversationId: String(resp?.OriginatorConversationID || ""),
      };
    } catch {
      return null;
    }
  }

  async findB2CRequestContextByConversationIds(
    conversationId: string,
    originatorConversationId: string,
  ): Promise<{ batchId?: string; itemId?: string; userId?: string } | null> {
    const conv = (conversationId || "").trim();
    const orig = (originatorConversationId || "").trim();
    if (!conv && !orig) return null;

    const matchCondition =
      conv && orig
        ? or(
            sql`(${mpesaAuditLogs.payload}::jsonb -> 'response' ->> 'ConversationID') = ${conv}`,
            sql`(${mpesaAuditLogs.payload}::jsonb -> 'response' ->> 'OriginatorConversationID') = ${orig}`,
          )!
        : conv
          ? sql`(${mpesaAuditLogs.payload}::jsonb -> 'response' ->> 'ConversationID') = ${conv}`
          : sql`(${mpesaAuditLogs.payload}::jsonb -> 'response' ->> 'OriginatorConversationID') = ${orig}`;

    const [log] = await db
      .select({ payload: mpesaAuditLogs.payload })
      .from(mpesaAuditLogs)
      .where(and(eq(mpesaAuditLogs.type, "b2c_request"), matchCondition))
      .orderBy(desc(mpesaAuditLogs.createdAt))
      .limit(1);

    if (!log?.payload) return null;
    try {
      const body = JSON.parse(log.payload) as Record<string, unknown>;
      return {
        batchId: typeof body.batchId === "string" ? body.batchId : undefined,
        itemId: typeof body.itemId === "string" ? body.itemId : undefined,
        userId: typeof body.userId === "string" ? body.userId : undefined,
      };
    } catch {
      return null;
    }
  }

  async getMpesaTransactionsPaginated(page = 1, pageSize = 20, paymentType?: string, status?: string, search?: string) {
    const offset = (page - 1) * pageSize;

    const conditions: SQL[] = [];
    if (paymentType && paymentType !== "all") {
      conditions.push(eq(mpesaTransactions.paymentType, paymentType));
    }
    if (status && status !== "all") {
      conditions.push(eq(mpesaTransactions.status, status));
    }
    if (search && search.trim()) {
      const term = `%${search.trim()}%`;
      conditions.push(or(
        like(mpesaTransactions.recipient, term),
        like(mpesaTransactions.recipientName, term),
        like(mpesaTransactions.mpesaTransactionId, term),
        like(mpesaTransactions.reference, term),
        like(mpesaTransactions.batchName, term),
      ) as SQL);
    }

    const whereClause = conditions.length > 0 ? and(...conditions) : undefined;

    const [totalResult, rows, lastSync] = await Promise.all([
      whereClause
        ? db.select({ count: sql<number>`COUNT(*)::int` }).from(mpesaTransactions).where(whereClause)
        : db.select({ count: sql<number>`COUNT(*)::int` }).from(mpesaTransactions),
      whereClause
        ? db.select().from(mpesaTransactions).where(whereClause).orderBy(desc(mpesaTransactions.syncedAt)).limit(pageSize).offset(offset)
        : db.select().from(mpesaTransactions).orderBy(desc(mpesaTransactions.syncedAt)).limit(pageSize).offset(offset),
      db.select({ syncedAt: mpesaTransactions.syncedAt }).from(mpesaTransactions).orderBy(desc(mpesaTransactions.syncedAt)).limit(1),
    ]);

    return {
      data: rows,
      total: totalResult[0]?.count ?? 0,
      page,
      pageSize,
      lastSyncedAt: lastSync[0]?.syncedAt?.toISOString() ?? null,
    };
  }

  async getAdminDisbursements(page = 1, pageSize = 20, paymentType?: string, status?: string) {
    const offset = (page - 1) * pageSize;

    // Build where conditions for batches
    const batchConditions: SQL[] = [];
    if (paymentType && paymentType !== "all") {
      batchConditions.push(eq(bulkPaymentBatches.paymentType, paymentType as any));
    }

    // Build where conditions for items
    const itemConditions: SQL[] = [];
    if (status && status !== "all") {
      itemConditions.push(eq(bulkPaymentItems.status, status as any));
    } else {
      // Default: show completed + failed only (exclude pending/processing)
      itemConditions.push(inArray(bulkPaymentItems.status, ["completed", "failed"]));
    }

    // Fetch items joined with batches
    const query = db
      .select({
        itemId: bulkPaymentItems.id,
        batchId: bulkPaymentBatches.id,
        paymentType: bulkPaymentBatches.paymentType,
        recipient: bulkPaymentItems.recipient,
        accountNumber: bulkPaymentItems.accountNumber,
        amount: bulkPaymentItems.amount,
        fee: bulkPaymentItems.fee,
        reference: bulkPaymentItems.reference,
        status: bulkPaymentItems.status,
        failureReason: bulkPaymentItems.failureReason,
        batchName: bulkPaymentBatches.createdByName,
        createdAt: bulkPaymentBatches.createdAt,
      })
      .from(bulkPaymentItems)
      .innerJoin(bulkPaymentBatches, eq(bulkPaymentItems.batchId, bulkPaymentBatches.id));

    const allConditions = [...batchConditions, ...itemConditions];
    const filteredQuery = allConditions.length > 0
      ? query.where(and(...allConditions))
      : query;

    const [totalResult, rows] = await Promise.all([
      (allConditions.length > 0
        ? db.select({ count: sql<number>`COUNT(*)::int` })
            .from(bulkPaymentItems)
            .innerJoin(bulkPaymentBatches, eq(bulkPaymentItems.batchId, bulkPaymentBatches.id))
            .where(and(...allConditions))
        : db.select({ count: sql<number>`COUNT(*)::int` })
            .from(bulkPaymentItems)
            .innerJoin(bulkPaymentBatches, eq(bulkPaymentItems.batchId, bulkPaymentBatches.id))
            .where(inArray(bulkPaymentItems.status, ["completed", "failed"]))
      ),
      filteredQuery.orderBy(desc(bulkPaymentBatches.createdAt)).limit(pageSize).offset(offset),
    ]);

    // Fetch b2c_result and b2b_result audit logs to extract recipient names and M-Pesa transaction IDs
    const resultLogs = await db.select()
      .from(mpesaAuditLogs)
      .where(inArray(mpesaAuditLogs.type, ["b2c_result", "b2b_result"]))
      .orderBy(desc(mpesaAuditLogs.createdAt))
      .limit(2000);

    // Build a map: itemId → { mpesaTransactionId, recipientName }
    const itemDataMap = new Map<string, { mpesaTransactionId: string; recipientName: string }>();
    for (const log of resultLogs) {
      try {
        const body = JSON.parse(log.payload);
        const result = body?.Result || {};
        const transactionId = String(result?.TransactionID || "");
        const params = result?.ResultParameters?.ResultParameter || [];
        const nameEntry = params.find((p: any) => p?.Key === "ReceiverPartyPublicName");
        const recipientName = String(nameEntry?.Value || "");
        const occasion = String(params.find((p: any) => p?.Key === "Occasion")?.Value || "");

        // Extract itemId from Occasion: "BATCH:xxx|ITEM:yyy"
        let itemId = "";
        if (occasion.includes("ITEM:")) {
          const [, itemPart] = occasion.split("|");
          itemId = (itemPart || "").replace("ITEM:", "").trim();
        }
        if (itemId && transactionId && !itemDataMap.has(itemId)) {
          itemDataMap.set(itemId, { mpesaTransactionId: transactionId, recipientName });
        }
      } catch {
        // skip malformed
      }
    }

    const data = rows.map((row) => {
      const extra = itemDataMap.get(row.itemId);
      return {
        itemId: row.itemId,
        batchId: row.batchId,
        paymentType: row.paymentType,
        recipient: row.recipient,
        accountNumber: row.accountNumber,
        amount: String(row.amount),
        fee: String(row.fee),
        reference: row.reference,
        status: row.status,
        failureReason: row.failureReason,
        mpesaTransactionId: extra?.mpesaTransactionId ?? null,
        recipientName: extra?.recipientName ?? null,
        batchName: row.batchName,
        createdByName: row.batchName,
        createdAt: row.createdAt.toISOString(),
      };
    });

    return { data, total: totalResult[0]?.count ?? 0, page, pageSize };
  }

  // ──────────── Notifications ────────────
  async createNotification(notification: InsertNotification): Promise<Notification> {
    const [result] = await db.insert(notifications).values({ ...notification, id: crypto.randomUUID() }).returning();
    return result;
  }

  async getNotifications(userId?: string, limit = 50): Promise<Notification[]> {
    if (userId) {
      return db.select().from(notifications)
        .where(eq(notifications.userId, userId))
        .orderBy(desc(notifications.createdAt))
        .limit(limit);
    }
    return db.select().from(notifications).orderBy(desc(notifications.createdAt)).limit(limit);
  }

  async getUnreadNotificationCount(userId?: string): Promise<number> {
    if (userId) {
      const result = await db.select({ count: sql<number>`count(*)::int` }).from(notifications)
        .where(and(eq(notifications.userId, userId), eq(notifications.isRead, false)));
      return result[0]?.count ?? 0;
    }
    const result = await db.select({ count: sql<number>`count(*)::int` }).from(notifications)
      .where(eq(notifications.isRead, false));
    return result[0]?.count ?? 0;
  }

  async markNotificationRead(id: string, userId: string): Promise<Notification | undefined> {
    const [result] = await db
      .update(notifications)
      .set({ isRead: true })
      .where(and(eq(notifications.id, id), eq(notifications.userId, userId)))
      .returning();
    return result;
  }

  async markAllNotificationsRead(userId?: string): Promise<void> {
    if (userId) {
      await db.update(notifications).set({ isRead: true }).where(and(eq(notifications.userId, userId), eq(notifications.isRead, false)));
    } else {
      await db.update(notifications).set({ isRead: true }).where(eq(notifications.isRead, false));
    }
  }

  // ──────────── Paginated Queries ────────────
  async getTransactionsPaginated(page: number, pageSize: number): Promise<PaginatedResult<Transaction>> {
    const offset = (page - 1) * pageSize;
    const [countResult] = await db.select({ count: sql<number>`count(*)::int` }).from(transactions);
    const data = await db.select().from(transactions).orderBy(desc(transactions.createdAt)).limit(pageSize).offset(offset);
    const total = countResult?.count ?? 0;
    return { data, total, page, pageSize, totalPages: Math.ceil(total / pageSize) };
  }

  async getApprovalsPaginated(page: number, pageSize: number, status?: string, makerIds?: string[]): Promise<PaginatedResult<Approval>> {
    const offset = (page - 1) * pageSize;
    const makerFilter = makerIds != null && makerIds.length > 0 ? inArray(approvals.makerId, makerIds) : undefined;
    if (makerIds != null && makerIds.length === 0) {
      return { data: [], total: 0, page, pageSize, totalPages: 0 };
    }
    const withMaker = (cond: SQL | undefined) => (makerFilter ? (cond ? and(cond, makerFilter) : makerFilter) : cond);
    if (status && status !== "all") {
      const statusCondition =
        status === "completed"
          ? inArray(approvals.status, ["approved", "rejected"])
          : eq(approvals.status, status as any);
      const where = withMaker(statusCondition);
      const [countResult] = await db.select({ count: sql<number>`count(*)::int` }).from(approvals).where(where!);
      const data = await db.select().from(approvals).where(where!).orderBy(desc(approvals.createdAt)).limit(pageSize).offset(offset);
      const total = countResult?.count ?? 0;
      return { data, total, page, pageSize, totalPages: Math.ceil(total / pageSize) };
    }
    const where = withMaker(undefined) ?? sql`1=1`;
    const [countResult] = await db.select({ count: sql<number>`count(*)::int` }).from(approvals).where(where);
    const data = await db.select().from(approvals).where(where).orderBy(desc(approvals.createdAt)).limit(pageSize).offset(offset);
    const total = countResult?.count ?? 0;
    return { data, total, page, pageSize, totalPages: Math.ceil(total / pageSize) };
  }

  async getApprovalCountsByStatus(makerIds?: string[]): Promise<{ pending_checker: number; pending_approver: number; approved: number; rejected: number }> {
    const makerFilter = makerIds != null && makerIds.length > 0 ? inArray(approvals.makerId, makerIds) : undefined;
    if (makerIds != null && makerIds.length === 0) {
      return { pending_checker: 0, pending_approver: 0, approved: 0, rejected: 0 };
    }
    const [row] = await db
      .select({
        pending_checker: sql<number>`count(*) filter (where ${approvals.status} = 'pending_checker')::int`,
        pending_approver: sql<number>`count(*) filter (where ${approvals.status} = 'pending_approver')::int`,
        approved: sql<number>`count(*) filter (where ${approvals.status} = 'approved')::int`,
        rejected: sql<number>`count(*) filter (where ${approvals.status} = 'rejected')::int`,
      })
      .from(approvals)
      .where(makerFilter ?? sql`1=1`);
    return {
      pending_checker: row?.pending_checker ?? 0,
      pending_approver: row?.pending_approver ?? 0,
      approved: row?.approved ?? 0,
      rejected: row?.rejected ?? 0,
    };
  }

  async getBatchesPaginated(page: number, pageSize: number, userId?: string): Promise<PaginatedResult<BulkPaymentBatch>> {
    const offset = (page - 1) * pageSize;
    if (userId) {
      const [countResult] = await db.select({ count: sql<number>`count(*)::int` }).from(bulkPaymentBatches).where(eq(bulkPaymentBatches.createdByUserId, userId));
      const data = await db.select().from(bulkPaymentBatches).where(eq(bulkPaymentBatches.createdByUserId, userId)).orderBy(desc(bulkPaymentBatches.createdAt)).limit(pageSize).offset(offset);
      const total = countResult?.count ?? 0;
      return { data, total, page, pageSize, totalPages: Math.ceil(total / pageSize) };
    }
    const [countResult] = await db.select({ count: sql<number>`count(*)::int` }).from(bulkPaymentBatches);
    const data = await db.select().from(bulkPaymentBatches).orderBy(desc(bulkPaymentBatches.createdAt)).limit(pageSize).offset(offset);
    const total = countResult?.count ?? 0;
    return { data, total, page, pageSize, totalPages: Math.ceil(total / pageSize) };
  }

  async getAllBatchesAdminPaginated(page: number, pageSize: number, status?: string): Promise<{ batches: BulkPaymentBatch[]; total: number; totalPages: number }> {
    const offset = (page - 1) * pageSize;
    const where = status && status !== "all" ? eq(bulkPaymentBatches.status, status as any) : undefined;
    const [{ total }] = await db.select({ total: sql<number>`count(*)::int` }).from(bulkPaymentBatches).where(where);
    const batches = await db.select().from(bulkPaymentBatches).where(where).orderBy(desc(bulkPaymentBatches.createdAt)).limit(pageSize).offset(offset);
    return { batches, total, totalPages: Math.ceil(total / pageSize) };
  }

  async getInvestmentsPaginated(page: number, pageSize: number): Promise<PaginatedResult<Investment>> {
    const offset = (page - 1) * pageSize;
    const [countResult] = await db.select({ count: sql<number>`count(*)::int` }).from(investments);
    const data = await db.select().from(investments).orderBy(desc(investments.createdAt)).limit(pageSize).offset(offset);
    const total = countResult?.count ?? 0;
    return { data, total, page, pageSize, totalPages: Math.ceil(total / pageSize) };
  }

  async getInvestorProfilesPaginated(page: number, pageSize: number): Promise<PaginatedResult<InvestorProfile>> {
    const offset = (page - 1) * pageSize;
    const [countResult] = await db.select({ count: sql<number>`count(*)::int` }).from(investorProfiles);
    const data = await db.select().from(investorProfiles).orderBy(desc(investorProfiles.createdAt)).limit(pageSize).offset(offset);
    const total = countResult?.count ?? 0;
    return { data, total, page, pageSize, totalPages: Math.ceil(total / pageSize) };
  }

  async getNotificationsPaginated(page: number, pageSize: number, userId?: string): Promise<PaginatedResult<Notification>> {
    const offset = (page - 1) * pageSize;
    if (userId) {
      const [countResult] = await db.select({ count: sql<number>`count(*)::int` }).from(notifications).where(eq(notifications.userId, userId));
      const data = await db.select().from(notifications).where(eq(notifications.userId, userId)).orderBy(desc(notifications.createdAt)).limit(pageSize).offset(offset);
      const total = countResult?.count ?? 0;
      return { data, total, page, pageSize, totalPages: Math.ceil(total / pageSize) };
    }
    const [countResult] = await db.select({ count: sql<number>`count(*)::int` }).from(notifications);
    const data = await db.select().from(notifications).orderBy(desc(notifications.createdAt)).limit(pageSize).offset(offset);
    const total = countResult?.count ?? 0;
    return { data, total, page, pageSize, totalPages: Math.ceil(total / pageSize) };
  }

  async getRecurringPaymentsPaginated(page: number, pageSize: number): Promise<PaginatedResult<RecurringPayment>> {
    const offset = (page - 1) * pageSize;
    const [countResult] = await db.select({ count: sql<number>`count(*)::int` }).from(recurringPayments);
    const data = await db.select().from(recurringPayments).orderBy(desc(recurringPayments.createdAt)).limit(pageSize).offset(offset);
    const total = countResult?.count ?? 0;
    return { data, total, page, pageSize, totalPages: Math.ceil(total / pageSize) };
  }

  async getRecurringPaymentsByBusinessPaginated(businessId: string, page: number, pageSize: number): Promise<PaginatedResult<RecurringPayment>> {
    const offset = (page - 1) * pageSize;
    const [countResult] = await db.select({ count: sql<number>`count(*)::int` }).from(recurringPayments).where(eq(recurringPayments.businessId, businessId));
    const data = await db.select().from(recurringPayments).where(eq(recurringPayments.businessId, businessId)).orderBy(desc(recurringPayments.createdAt)).limit(pageSize).offset(offset);
    const total = countResult?.count ?? 0;
    return { data, total, page, pageSize, totalPages: Math.ceil(total / pageSize) };
  }

  async getRecurringPaymentsByUserPaginated(userId: string, page: number, pageSize: number): Promise<PaginatedResult<RecurringPayment>> {
    const offset = (page - 1) * pageSize;
    const [countResult] = await db.select({ count: sql<number>`count(*)::int` }).from(recurringPayments).where(eq(recurringPayments.userId, userId));
    const data = await db.select().from(recurringPayments).where(eq(recurringPayments.userId, userId)).orderBy(desc(recurringPayments.createdAt)).limit(pageSize).offset(offset);
    const total = countResult?.count ?? 0;
    return { data, total, page, pageSize, totalPages: Math.ceil(total / pageSize) };
  }

  async getPaymentLinksPaginated(page: number, pageSize: number, userId?: string): Promise<PaginatedResult<PaymentLink>> {
    const offset = (page - 1) * pageSize;
    if (userId) {
      const [countResult] = await db.select({ count: sql<number>`count(*)::int` }).from(paymentLinks).where(eq(paymentLinks.createdByUserId, userId));
      const data = await db.select().from(paymentLinks).where(eq(paymentLinks.createdByUserId, userId)).orderBy(desc(paymentLinks.createdAt)).limit(pageSize).offset(offset);
      const total = countResult?.count ?? 0;
      return { data, total, page, pageSize, totalPages: Math.ceil(total / pageSize) };
    }
    const [countResult] = await db.select({ count: sql<number>`count(*)::int` }).from(paymentLinks);
    const data = await db.select().from(paymentLinks).orderBy(desc(paymentLinks.createdAt)).limit(pageSize).offset(offset);
    const total = countResult?.count ?? 0;
    return { data, total, page, pageSize, totalPages: Math.ceil(total / pageSize) };
  }

  async getAdvancesPaginated(page: number, pageSize: number): Promise<PaginatedResult<CashAdvance>> {
    const offset = (page - 1) * pageSize;
    const [countResult] = await db.select({ count: sql<number>`count(*)::int` }).from(cashAdvances);
    const data = await db.select().from(cashAdvances).orderBy(desc(cashAdvances.createdAt)).limit(pageSize).offset(offset);
    const total = countResult?.count ?? 0;
    return { data, total, page, pageSize, totalPages: Math.ceil(total / pageSize) };
  }

  // ──────────── Admin Dashboard ────────────
  async getAllBusinesses(): Promise<(Business & { user?: User })[]> {
    const allBusinesses = await db
      .select()
      .from(businesses)
      .where(eq(businesses.isSandbox, false))
      .orderBy(desc(businesses.createdAt));
    const result: (Business & { user?: User })[] = [];
    for (const biz of allBusinesses) {
      const [user] = await db.select().from(users).where(eq(users.id, biz.userId));
      result.push({ ...biz, user });
    }
    return result;
  }

  async getBusinessesPaginated(limit: number, offset: number): Promise<{ data: (Business & { user?: User })[]; total: number }> {
    const [rows, totalResult] = await Promise.all([
      db.select().from(businesses).where(eq(businesses.isSandbox, false)).orderBy(desc(businesses.createdAt)).limit(limit).offset(offset),
      db.select({ count: sql<number>`count(*)::int` }).from(businesses).where(eq(businesses.isSandbox, false)),
    ]);
    const userIds = [...new Set(rows.map(b => b.userId).filter(Boolean))];
    const userRows = userIds.length > 0 ? await db.select().from(users).where(inArray(users.id, userIds as string[])) : [];
    const userMap = new Map(userRows.map(u => [u.id, u]));
    return {
      data: rows.map(b => ({ ...b, user: userMap.get(b.userId) })),
      total: totalResult[0]?.count ?? 0,
    };
  }

  async getUsersPaginated(limit: number, offset: number): Promise<{ data: (Omit<User, "passwordHash"> & { companyName: string | null; companyId: string | null })[]; total: number }> {
    const [rows, totalResult] = await Promise.all([
      db.select().from(users).orderBy(desc(users.createdAt)).limit(limit).offset(offset),
      db.select({ count: sql<number>`count(*)::int` }).from(users),
    ]);

    // Batch-fetch businesses for the current page: by businessId (member) and by userId (owner)
    const memberBizIds = [...new Set(rows.map(u => u.businessId).filter(Boolean))] as string[];
    const userIds = rows.map(u => u.id);

    const [memberBizList, ownedBizList] = await Promise.all([
      memberBizIds.length > 0
        ? db.select({ id: businesses.id, name: businesses.businessName }).from(businesses).where(inArray(businesses.id, memberBizIds))
        : Promise.resolve([]),
      userIds.length > 0
        ? db.select({ userId: businesses.userId, id: businesses.id, name: businesses.businessName }).from(businesses).where(inArray(businesses.userId, userIds))
        : Promise.resolve([]),
    ]);

    const memberBizMap = new Map(memberBizList.map(b => [b.id, b]));
    const ownedBizMap = new Map(ownedBizList.map(b => [b.userId!, b]));

    return {
      data: rows.map(({ passwordHash, ...u }) => {
        const memberBiz = u.businessId ? memberBizMap.get(u.businessId) : undefined;
        const ownedBiz = ownedBizMap.get(u.id);
        return {
          ...u,
          companyName: memberBiz?.name || ownedBiz?.name || null,
          companyId: memberBiz?.id || ownedBiz?.id || null,
        };
      }),
      total: totalResult[0]?.count ?? 0,
    };
  }

  async getAdminTransactionsPaginated(limit: number, offset: number): Promise<{ data: Transaction[]; total: number }> {
    const [data, totalResult] = await Promise.all([
      db.select().from(transactions).orderBy(desc(transactions.createdAt)).limit(limit).offset(offset),
      db.select({ count: sql<number>`count(*)::int` }).from(transactions),
    ]);
    return { data, total: totalResult[0]?.count ?? 0 };
  }

  async getAdminDashboardStats() {
    const nonSandboxUserWhere = sql`
      ${users.businessId} IS NULL
      OR ${users.businessId} IN (
        SELECT id FROM businesses WHERE COALESCE(is_sandbox, false) = false
      )
    `;
    const nonSandboxBatchWhere = sql`
      ${bulkPaymentBatches.createdByUserId} IS NOT NULL
      AND ${bulkPaymentBatches.createdByUserId} IN (
        SELECT users.id
        FROM users
        LEFT JOIN businesses ON businesses.id = users.business_id
        WHERE COALESCE(businesses.is_sandbox, false) = false
      )
    `;
    const nonSandboxApprovalWhere = sql`
      ${approvals.makerId} IN (
        SELECT users.id
        FROM users
        LEFT JOIN businesses ON businesses.id = users.business_id
        WHERE COALESCE(businesses.is_sandbox, false) = false
      )
    `;
    const nonSandboxLinkWhere = sql`
      ${paymentLinks.createdByUserId} IS NOT NULL
      AND ${paymentLinks.createdByUserId} IN (
        SELECT users.id
        FROM users
        LEFT JOIN businesses ON businesses.id = users.business_id
        WHERE COALESCE(businesses.is_sandbox, false) = false
      )
    `;

    const [
      [userCount],
      [activeUserCount],
      [bizCount],
      [kycPendingCount],
      [kycApprovedCount],
      [kycRejectedCount],
      [batchTxnStats],
      [linkTxnStats],
      [advTotal],
      [advActive],
      [advPending],
      [invTotal],
      [invActive],
      [batchStats],
      [pendingAppr],
      [approvedAppr],
      [rejectedAppr],
      [linkStats],
      recentUsers,
      rawTransactions,
    ] = await Promise.all([
      db.select({ count: sql<number>`COUNT(*)` }).from(users).where(nonSandboxUserWhere),
      db.select({ count: sql<number>`COUNT(*)` }).from(users).where(and(eq(users.status, "active"), nonSandboxUserWhere)),
      db.select({ count: sql<number>`COUNT(*)` }).from(businesses).where(eq(businesses.isSandbox, false)),
      db.select({ count: sql<number>`COUNT(*)` }).from(businesses).where(and(eq(businesses.kycStatus, "pending"), eq(businesses.isSandbox, false))),
      db.select({ count: sql<number>`COUNT(*)` }).from(businesses).where(and(eq(businesses.kycStatus, "approved"), eq(businesses.isSandbox, false))),
      db.select({ count: sql<number>`COUNT(*)` }).from(businesses).where(and(eq(businesses.kycStatus, "rejected"), eq(businesses.isSandbox, false))),
      // Keep dashboard payment volume/count aligned with Admin Payments page:
      // - Batch payments from bulk_payment_batches.total_amount (non-sandbox only)
      // - Payment links counted only when paid, volume from paid_amount
      db.select({
        totalCount: sql<number>`COUNT(*)::int`,
        totalVolume: sql<string>`COALESCE(SUM(${bulkPaymentBatches.totalAmount}::numeric), 0)::text`,
      }).from(bulkPaymentBatches).where(nonSandboxBatchWhere),
      db.select({
        totalCount: sql<number>`COUNT(*) FILTER (WHERE ${paymentLinks.status} = 'paid')::int`,
        totalVolume: sql<string>`COALESCE(SUM(${paymentLinks.paidAmount}::numeric), 0)::text`,
      }).from(paymentLinks).where(nonSandboxLinkWhere),
      db.select({ total: sql<string>`COALESCE(SUM(amount::numeric), 0)::text` }).from(cashAdvances),
      db.select({ count: sql<number>`COUNT(*)` }).from(cashAdvances).where(eq(cashAdvances.status, "active")),
      db.select({ count: sql<number>`COUNT(*)` }).from(cashAdvances).where(eq(cashAdvances.status, "pending")),
      db.select({ total: sql<string>`COALESCE(SUM(amount_invested::numeric), 0)::text` }).from(investments),
      db.select({ count: sql<number>`COUNT(*)` }).from(investments).where(eq(investments.status, "active")),
      db.select({
        count: sql<number>`COUNT(*)::int`,
        totalAmount: sql<string>`COALESCE(SUM(${bulkPaymentBatches.totalAmount}::numeric), 0)::text`,
      }).from(bulkPaymentBatches).where(nonSandboxBatchWhere),
      db.select({ count: sql<number>`COUNT(*)` }).from(approvals).where(sql`${nonSandboxApprovalWhere} AND status IN ('pending_checker', 'pending_approver')`),
      db.select({ count: sql<number>`COUNT(*)` }).from(approvals).where(and(eq(approvals.status, "approved" as any), nonSandboxApprovalWhere)),
      db.select({ count: sql<number>`COUNT(*)` }).from(approvals).where(and(eq(approvals.status, "rejected" as any), nonSandboxApprovalWhere)),
      db.select({
        totalLinks: sql<number>`COUNT(*)::int`,
        activeLinks: sql<number>`COUNT(*) FILTER (WHERE ${paymentLinks.status} = 'active')::int`,
        paidLinks: sql<number>`COUNT(*) FILTER (WHERE ${paymentLinks.status} = 'paid')::int`,
      }).from(paymentLinks).where(nonSandboxLinkWhere),
      db.select().from(users).where(nonSandboxUserWhere).orderBy(desc(users.createdAt)).limit(5),
      db.select().from(transactions).orderBy(desc(transactions.createdAt)).limit(10),
    ]);

    // ── Enrich transactions with actor + company info ──────────────────────
    const refs = rawTransactions.map(t => t.reference).filter(Boolean);
    const enrichMap = new Map<string, { actorName: string | null; actorUserId: string | null; actorEmail: string | null; businessName: string | null; businessId: string | null }>();

    if (refs.length > 0) {
      // Match wallet topups by mpesa receipt number (the transaction reference for STK push)
      const topupMatches = await db
        .select({ ref: walletTopups.mpesaReceiptNumber, userId: walletTopups.userId, userName: walletTopups.userName })
        .from(walletTopups)
        .where(inArray(walletTopups.mpesaReceiptNumber, refs));
      for (const t of topupMatches) {
        if (t.ref) enrichMap.set(t.ref, { actorName: t.userName, actorUserId: t.userId, actorEmail: null, businessName: null, businessId: null });
      }

      // Match batch items by reference (M-Pesa transactionId stored in bulkPaymentItems.reference)
      const batchMatches = await db
        .select({ ref: bulkPaymentItems.reference, actorName: bulkPaymentBatches.createdByName, actorUserId: bulkPaymentBatches.createdByUserId })
        .from(bulkPaymentItems)
        .innerJoin(bulkPaymentBatches, eq(bulkPaymentItems.batchId, bulkPaymentBatches.id))
        .where(inArray(bulkPaymentItems.reference, refs));
      for (const b of batchMatches) {
        if (b.ref && !enrichMap.has(b.ref)) {
          enrichMap.set(b.ref, { actorName: b.actorName, actorUserId: b.actorUserId, actorEmail: null, businessName: null, businessId: null });
        }
      }

      // Handle "B2C-{itemId}" / "B2B-{itemId}" patterns — strip prefix and look up by item id
      const prefixedRefs = refs.filter(r => r.startsWith("B2C-") || r.startsWith("B2B-"));
      if (prefixedRefs.length > 0) {
        const itemIds = prefixedRefs.map(r => r.substring(4));
        const itemMatches = await db
          .select({ id: bulkPaymentItems.id, actorName: bulkPaymentBatches.createdByName, actorUserId: bulkPaymentBatches.createdByUserId })
          .from(bulkPaymentItems)
          .innerJoin(bulkPaymentBatches, eq(bulkPaymentItems.batchId, bulkPaymentBatches.id))
          .where(inArray(bulkPaymentItems.id, itemIds));
        for (const m of itemMatches) {
          const origRef = prefixedRefs.find(r => r.endsWith(m.id));
          if (origRef && !enrichMap.has(origRef)) {
            enrichMap.set(origRef, { actorName: m.actorName, actorUserId: m.actorUserId, actorEmail: null, businessName: null, businessId: null });
          }
        }
      }
    }

    // Resolve actor emails + business names for collected userIds — single JOIN handles
    // both owners (businesses.userId = user.id) and members (users.businessId = businesses.id)
    const actorUserIdSet: string[] = [];
    enrichMap.forEach((info) => { if (info.actorUserId) actorUserIdSet.push(info.actorUserId); });
    const uniqueActorUserIds = Array.from(new Set(actorUserIdSet));

    if (uniqueActorUserIds.length > 0) {
      // One query: LEFT JOIN on ownership AND on membership simultaneously
      const userBizRows = await db
        .select({
          userId: users.id,
          email: users.email,
          ownedBizId: sql<string | null>`biz_owned.id`,
          ownedBizName: sql<string | null>`biz_owned.business_name`,
          memberBizId: sql<string | null>`biz_member.id`,
          memberBizName: sql<string | null>`biz_member.business_name`,
        })
        .from(users)
        .leftJoin(
          sql`businesses biz_owned`,
          sql`biz_owned.user_id = ${users.id}`,
        )
        .leftJoin(
          sql`businesses biz_member`,
          sql`biz_member.id = ${users.businessId}`,
        )
        .where(inArray(users.id, uniqueActorUserIds));

      const userInfoMap = new Map(userBizRows.map(r => [r.userId, r]));

      enrichMap.forEach((info) => {
        if (!info.actorUserId) return;
        const row = userInfoMap.get(info.actorUserId);
        if (!row) return;
        info.actorEmail = row.email || null;
        // Prefer owned business; fall back to the company the user is a member of
        if (row.ownedBizId) {
          info.businessName = row.ownedBizName;
          info.businessId = row.ownedBizId;
        } else if (row.memberBizId) {
          info.businessName = row.memberBizName;
          info.businessId = row.memberBizId;
        }
      });
    }

    const recentTransactions: EnrichedAdminTransaction[] = rawTransactions.map(t => ({
      ...t,
      ...( enrichMap.get(t.reference) ?? { actorName: null, actorUserId: null, actorEmail: null, businessName: null, businessId: null }),
    }));

    const dashboardTxnCount =
      Number(batchTxnStats?.totalCount || 0) + Number(linkTxnStats?.totalCount || 0);
    const dashboardTxnVolume =
      Number(batchTxnStats?.totalVolume || 0) + Number(linkTxnStats?.totalVolume || 0);

    return {
      totalUsers: Number(userCount?.count || 0),
      activeUsers: Number(activeUserCount?.count || 0),
      totalBusinesses: Number(bizCount?.count || 0),
      kycPending: Number(kycPendingCount?.count || 0),
      kycApproved: Number(kycApprovedCount?.count || 0),
      kycRejected: Number(kycRejectedCount?.count || 0),
      totalTransactionVolume: String(dashboardTxnVolume),
      totalTransactionCount: dashboardTxnCount,
      totalAdvancesIssued: advTotal?.total || "0",
      activeAdvances: Number(advActive?.count || 0),
      pendingAdvances: Number(advPending?.count || 0),
      totalInvestments: invTotal?.total || "0",
      activeInvestments: Number(invActive?.count || 0),
      totalPaymentBatches: Number(batchStats?.count || 0),
      totalPaymentBatchAmount: batchStats?.totalAmount || "0",
      pendingApprovals: Number(pendingAppr?.count || 0),
      approvedCount: Number(approvedAppr?.count || 0),
      rejectedCount: Number(rejectedAppr?.count || 0),
      totalPaymentLinks: Number(linkStats?.totalLinks || 0),
      activePaymentLinks: Number(linkStats?.activeLinks || 0),
      paidPaymentLinks: Number(linkStats?.paidLinks || 0),
      recentUsers,
      recentTransactions,
    };
  }

  // ──────────── Subscriptions ────────────
  async getSubscriptions(): Promise<Subscription[]> {
    return db
      .select()
      .from(subscriptions)
      .where(sql`${subscriptions.businessId} NOT LIKE 'demo-%'`)
      .orderBy(desc(subscriptions.createdAt));
  }

  async getSubscriptionById(id: string): Promise<Subscription | undefined> {
    const [result] = await db.select().from(subscriptions).where(eq(subscriptions.id, id));
    return result;
  }

  async createSubscription(sub: InsertSubscription): Promise<Subscription> {
    const [result] = await db.insert(subscriptions).values({ ...sub, id: crypto.randomUUID() }).returning();
    return result;
  }

  async updateSubscriptionStatus(id: string, status: string): Promise<Subscription | undefined> {
    const updates: any = { status };
    if (status === "cancelled") updates.endDate = new Date();
    const [result] = await db.update(subscriptions).set(updates).where(eq(subscriptions.id, id)).returning();
    return result;
  }

  async getSubscriptionByBusinessId(businessId: string): Promise<Subscription | undefined> {
    const [sub] = await db.select().from(subscriptions)
      .where(and(eq(subscriptions.businessId, businessId), eq(subscriptions.status, "active")))
      .orderBy(desc(subscriptions.createdAt))
      .limit(1);
    return sub;
  }

  async cancelSubscription(subscriptionId: string): Promise<void> {
    await db.update(subscriptions)
      .set({ status: "cancelled", endDate: new Date() })
      .where(eq(subscriptions.id, subscriptionId));
  }

  async purchaseSubscription(opts: {
    businessId: string;
    businessOwnerUserId: string;
    businessOwnerName: string;
    businessName: string;
    plan: "starter" | "business" | "enterprise";
    billingCycle: "monthly" | "annually";
    amount: number;
  }): Promise<{ subscription: Subscription; invoice: Invoice }> {
    const { businessId, businessOwnerUserId, businessOwnerName, businessName, plan, billingCycle, amount } = opts;

    // 1. Check wallet balance
    const wallet = await this.getUserWalletBalance(businessOwnerUserId);
    if (Number(wallet.availableBalance) < amount) {
      throw new Error("Insufficient wallet balance");
    }

    // 2. Deduct from wallet (negative topup = deduction)
    const ref = `SUB-${crypto.randomUUID().slice(0, 8).toUpperCase()}`;
    await this.createWalletTopup({
      userId: businessOwnerUserId,
      userName: businessOwnerName,
      amount: (-amount).toFixed(2),
      method: "subscription_fee",
      reference: ref,
      status: "approved" as any,
    });

    // 3. Cancel any existing active subscription for this business
    const existing = await this.getSubscriptionByBusinessId(businessId);
    if (existing) {
      await this.cancelSubscription(existing.id);
    }

    // 4. Compute next billing date
    const now = new Date();
    const nextBillingDate = new Date(now);
    if (billingCycle === "annually") {
      nextBillingDate.setFullYear(nextBillingDate.getFullYear() + 1);
    } else {
      nextBillingDate.setMonth(nextBillingDate.getMonth() + 1);
    }

    // 5. Create subscription
    const subscription = await this.createSubscription({
      businessId,
      businessName,
      plan: plan as any,
      billingCycle: billingCycle === "annually" ? "annually" as any : "monthly" as any,
      amount: amount.toFixed(2),
      status: "active" as any,
      startDate: now,
      nextBillingDate,
    });

    // 6. Create paid invoice
    const yyyymm = `${now.getFullYear()}${String(now.getMonth() + 1).padStart(2, "0")}`;
    const invoiceNumber = `INV-${yyyymm}-${crypto.randomUUID().slice(0, 6).toUpperCase()}`;
    const periodEnd = new Date(nextBillingDate);
    periodEnd.setDate(periodEnd.getDate() - 1);
    const invoiceRaw = await this.createInvoice({
      subscriptionId: subscription.id,
      businessId,
      businessName,
      invoiceNumber,
      amount: amount.toFixed(2),
      tax: "0",
      totalAmount: amount.toFixed(2),
      status: "paid" as any,
      dueDate: now,
      billingPeriodStart: now,
      billingPeriodEnd: periodEnd,
    });
    // Set paidAt separately since insertInvoiceSchema omits it
    const invoice = await this.updateInvoiceStatus(invoiceRaw.id, "paid", now) ?? invoiceRaw;

    return { subscription, invoice };
  }

  async seedDefaultPlanSettings(): Promise<void> {
    const existing = await this.getPlatformSetting("plan.starter");
    if (existing) return; // already seeded

    const defaults = {
      "plan.starter": JSON.stringify({
        priceMonthly: 4999,
        priceAnnual: 49990,
        userLimit: 5,
        paymentLinksMonthly: 50,
        hasRecurring: false,
        hasMakerChecker: false,
        cashAdvanceLimit: 0,
        features: [
          "Up to 5 team members",
          "Mobile Money payments (M-Pesa)",
          "Single & bulk disbursements",
          "Payment links (50/month)",
          "Basic transaction reports",
          "Email support (48hr response)",
        ],
        excludedFeatures: [
          "Recurring payments",
          "Cash advance / overdraft",
          "Maker-checker approvals",
          "Investment portal",
          "Custom API access",
          "Dedicated account manager",
        ],
      }),
      "plan.business": JSON.stringify({
        priceMonthly: 14999,
        priceAnnual: 149990,
        userLimit: 25,
        paymentLinksMonthly: -1,
        hasRecurring: true,
        hasMakerChecker: true,
        cashAdvanceLimit: 2000000,
        popular: true,
        features: [
          "Up to 25 team members",
          "All M-Pesa channels (Send, Buy Goods, Paybill)",
          "Unlimited bulk disbursements",
          "Unlimited payment links",
          "Recurring payment automation",
          "Maker-checker approval workflow",
          "Advanced analytics & reports",
          "Priority support (12hr response)",
          "Cash advance up to KES 2M",
        ],
        excludedFeatures: [
          "Investment portal",
          "Custom API access",
          "Dedicated account manager",
        ],
      }),
      "plan.enterprise": JSON.stringify({
        priceMonthly: 49999,
        priceAnnual: 499990,
        userLimit: -1,
        paymentLinksMonthly: -1,
        hasRecurring: true,
        hasMakerChecker: true,
        cashAdvanceLimit: 10000000,
        contactSales: true,
        features: [
          "Unlimited team members",
          "All payment channels & methods",
          "Unlimited bulk disbursements",
          "Unlimited payment links",
          "Recurring payment automation",
          "Multi-level approval workflows",
          "Real-time analytics & custom dashboards",
          "Cash advance up to KES 10M",
          "Investment portal with returns tracking",
          "Full REST API access & webhooks",
          "Dedicated account manager",
          "24/7 priority support & SLA guarantee",
        ],
        excludedFeatures: [],
      }),
    };

    for (const [key, value] of Object.entries(defaults)) {
      await this.setPlatformSetting(key, value, "system");
    }
  }

  // ──────────── Invoices ────────────
  async getInvoices(): Promise<Invoice[]> {
    return db
      .select()
      .from(invoices)
      .where(sql`${invoices.businessId} NOT LIKE 'demo-%'`)
      .orderBy(desc(invoices.createdAt));
  }

  async getInvoiceById(id: string): Promise<Invoice | undefined> {
    const [result] = await db.select().from(invoices).where(eq(invoices.id, id));
    return result;
  }

  async getInvoicesByBusiness(businessId: string): Promise<Invoice[]> {
    return db.select().from(invoices).where(eq(invoices.businessId, businessId)).orderBy(desc(invoices.createdAt));
  }

  async getBusinessSubscriptionInvoices(businessId: string): Promise<Invoice[]> {
    return db.select().from(invoices)
      .where(eq(invoices.businessId, businessId))
      .orderBy(desc(invoices.createdAt));
  }

  async createInvoice(invoice: InsertInvoice): Promise<Invoice> {
    const [result] = await db.insert(invoices).values({ ...invoice, id: crypto.randomUUID() }).returning();
    return result;
  }

  async updateInvoiceStatus(id: string, status: string, paidAt?: Date): Promise<Invoice | undefined> {
    const updates: any = { status };
    if (paidAt) updates.paidAt = paidAt;
    else if (status === "paid") updates.paidAt = new Date();
    const [result] = await db.update(invoices).set(updates).where(eq(invoices.id, id)).returning();
    return result;
  }

  // ──────────── AR core (tenant scoped) ────────────
  async createArCustomer(input: InsertArCustomer): Promise<ArCustomer> {
    const [result] = await db.insert(arCustomers).values({ ...input, id: crypto.randomUUID() }).returning();
    return result;
  }

  async updateArCustomer(id: string, businessId: string, patch: Partial<InsertArCustomer>): Promise<ArCustomer | undefined> {
    const [result] = await db
      .update(arCustomers)
      .set({ ...patch, updatedAt: new Date() })
      .where(and(eq(arCustomers.id, id), eq(arCustomers.businessId, businessId)))
      .returning();
    return result;
  }

  async getArCustomerById(id: string, businessId: string): Promise<ArCustomer | undefined> {
    const [result] = await db.select().from(arCustomers).where(and(eq(arCustomers.id, id), eq(arCustomers.businessId, businessId)));
    return result;
  }

  async getArCustomerByErpId(erpCustomerId: string, businessId: string): Promise<ArCustomer | undefined> {
    const [result] = await db
      .select()
      .from(arCustomers)
      .where(and(eq(arCustomers.erpCustomerId, erpCustomerId), eq(arCustomers.businessId, businessId)));
    return result;
  }

  async listArCustomersByBusiness(businessId: string): Promise<ArCustomer[]> {
    return db.select().from(arCustomers).where(eq(arCustomers.businessId, businessId)).orderBy(asc(arCustomers.customerName));
  }

  async createArInvoice(input: InsertArInvoice): Promise<ArInvoice> {
    const [result] = await db.insert(arInvoices).values({ ...input, id: crypto.randomUUID() }).returning();
    return result;
  }

  async updateArInvoice(id: string, businessId: string, patch: Partial<InsertArInvoice>): Promise<ArInvoice | undefined> {
    const [result] = await db
      .update(arInvoices)
      .set({ ...patch, updatedAt: new Date() })
      .where(and(eq(arInvoices.id, id), eq(arInvoices.businessId, businessId)))
      .returning();
    return result;
  }

  async getArInvoiceById(id: string, businessId: string): Promise<ArInvoice | undefined> {
    const [result] = await db.select().from(arInvoices).where(and(eq(arInvoices.id, id), eq(arInvoices.businessId, businessId)));
    return result;
  }

  async getArInvoiceByErpId(erpInvoiceId: string, businessId: string): Promise<ArInvoice | undefined> {
    const [result] = await db
      .select()
      .from(arInvoices)
      .where(and(eq(arInvoices.erpInvoiceId, erpInvoiceId), eq(arInvoices.businessId, businessId)));
    return result;
  }

  async listArInvoicesByBusiness(businessId: string): Promise<ArInvoice[]> {
    return db.select().from(arInvoices).where(eq(arInvoices.businessId, businessId)).orderBy(desc(arInvoices.createdAt));
  }

  async replaceArInvoiceLines(arInvoiceId: string, lines: Omit<InsertArInvoiceLine, "arInvoiceId">[]): Promise<ArInvoiceLine[]> {
    await db.delete(arInvoiceLines).where(eq(arInvoiceLines.arInvoiceId, arInvoiceId));
    if (!lines.length) return [];
    return db
      .insert(arInvoiceLines)
      .values(lines.map((line) => ({ ...line, id: crypto.randomUUID(), arInvoiceId })))
      .returning();
  }

  async listArInvoiceLines(arInvoiceId: string): Promise<ArInvoiceLine[]> {
    return db.select().from(arInvoiceLines).where(eq(arInvoiceLines.arInvoiceId, arInvoiceId)).orderBy(asc(arInvoiceLines.lineNo));
  }

  async createArPayment(input: InsertArPayment): Promise<ArPayment> {
    const [result] = await db.insert(arPayments).values({ ...input, id: crypto.randomUUID() }).returning();
    return result;
  }

  async getArPaymentById(id: string, businessId: string): Promise<ArPayment | undefined> {
    const [result] = await db.select().from(arPayments).where(and(eq(arPayments.id, id), eq(arPayments.businessId, businessId)));
    return result;
  }

  async getArPaymentByPaymentLinkId(paymentLinkId: string): Promise<ArPayment | undefined> {
    const [result] = await db.select().from(arPayments).where(eq(arPayments.paymentLinkId, paymentLinkId)).limit(1);
    return result;
  }

  async listArPaymentsByBusiness(businessId: string): Promise<ArPayment[]> {
    return db.select().from(arPayments).where(eq(arPayments.businessId, businessId)).orderBy(desc(arPayments.paidAt));
  }

  async createArAllocation(input: InsertArAllocation): Promise<ArAllocation> {
    const [result] = await db.insert(arAllocations).values({ ...input, id: crypto.randomUUID() }).returning();
    return result;
  }

  async listArAllocationsByInvoice(arInvoiceId: string): Promise<ArAllocation[]> {
    return db.select().from(arAllocations).where(eq(arAllocations.arInvoiceId, arInvoiceId)).orderBy(desc(arAllocations.createdAt));
  }

  async listArAllocationsByPayment(arPaymentId: string): Promise<ArAllocation[]> {
    return db.select().from(arAllocations).where(eq(arAllocations.arPaymentId, arPaymentId)).orderBy(desc(arAllocations.createdAt));
  }

  async createArStatementEntry(input: InsertArStatementEntry): Promise<ArStatementEntry> {
    const [result] = await db.insert(arStatementEntries).values({ ...input, id: crypto.randomUUID() }).returning();
    return result;
  }

  async listArStatementEntries(
    businessId: string,
    opts?: { arCustomerId?: string; from?: Date; to?: Date },
  ): Promise<ArStatementEntry[]> {
    const filters: SQL[] = [eq(arStatementEntries.businessId, businessId)];
    if (opts?.arCustomerId) filters.push(eq(arStatementEntries.arCustomerId, opts.arCustomerId));
    if (opts?.from) filters.push(gte(arStatementEntries.postedAt, opts.from));
    if (opts?.to) filters.push(lte(arStatementEntries.postedAt, opts.to));
    return db
      .select()
      .from(arStatementEntries)
      .where(and(...filters))
      .orderBy(desc(arStatementEntries.postedAt), desc(arStatementEntries.createdAt));
  }

  async getArUnallocatedCreditByCustomer(businessId: string, arCustomerId: string): Promise<string> {
    const totalPaymentsQ = await db
      .select({
        total: sql<string>`COALESCE(SUM(${arPayments.amount}::numeric), 0)::text`,
      })
      .from(arPayments)
      .where(and(eq(arPayments.businessId, businessId), eq(arPayments.arCustomerId, arCustomerId), eq(arPayments.status, "posted")));
    const totalAllocatedQ = await db
      .select({
        total: sql<string>`COALESCE(SUM(${arAllocations.amount}::numeric), 0)::text`,
      })
      .from(arAllocations)
      .innerJoin(arPayments, eq(arPayments.id, arAllocations.arPaymentId))
      .where(and(eq(arPayments.businessId, businessId), eq(arPayments.arCustomerId, arCustomerId)));
    const payments = Number(totalPaymentsQ[0]?.total ?? "0");
    const allocated = Number(totalAllocatedQ[0]?.total ?? "0");
    return String(Math.max(0, payments - allocated));
  }

  // ──────────── Support Tickets ────────────
  async getTickets(status?: string): Promise<SupportTicket[]> {
    const nonDemoRealTicketFilter = sql`
      COALESCE(${supportTickets.businessId}, '') NOT LIKE 'demo-%'
      AND (${supportTickets.businessId} IS NOT NULL OR ${supportTickets.userId} IS NOT NULL)
    `;
    if (status) {
      return db
        .select()
        .from(supportTickets)
        .where(and(eq(supportTickets.status, status as any), nonDemoRealTicketFilter))
        .orderBy(desc(supportTickets.createdAt));
    }
    return db
      .select()
      .from(supportTickets)
      .where(nonDemoRealTicketFilter)
      .orderBy(desc(supportTickets.createdAt));
  }

  async getTicketById(id: string): Promise<SupportTicket | undefined> {
    const [result] = await db.select().from(supportTickets).where(eq(supportTickets.id, id));
    return result;
  }

  async createTicket(ticket: InsertSupportTicket): Promise<SupportTicket> {
    const [result] = await db.insert(supportTickets).values({ ...ticket, id: crypto.randomUUID() }).returning();
    return result;
  }

  async updateTicketStatus(id: string, status: string, resolution?: string): Promise<SupportTicket | undefined> {
    const updates: any = { status, updatedAt: new Date() };
    if (resolution) updates.resolution = resolution;
    if (status === "resolved" || status === "closed") updates.resolvedAt = new Date();
    const [result] = await db.update(supportTickets).set(updates).where(eq(supportTickets.id, id)).returning();
    return result;
  }

  async assignTicket(id: string, assignedTo: string, assignedToName: string): Promise<SupportTicket | undefined> {
    const [result] = await db.update(supportTickets).set({ assignedTo, assignedToName, updatedAt: new Date() }).where(eq(supportTickets.id, id)).returning();
    return result;
  }

  // ──────────── Ticket Comments ────────────
  async getTicketComments(ticketId: string): Promise<TicketComment[]> {
    return db.select().from(ticketComments).where(eq(ticketComments.ticketId, ticketId)).orderBy(ticketComments.createdAt);
  }

  async createTicketComment(comment: InsertTicketComment): Promise<TicketComment> {
    const [result] = await db.insert(ticketComments).values({ ...comment, id: crypto.randomUUID() }).returning();
    return result;
  }

  // ──────────── Imported Transactions ────────────
  async createImportedTransactions(rows: InsertImportedTransaction[]): Promise<ImportedTransaction[]> {
    if (rows.length === 0) return [];
    const withIds = rows.map(r => ({ ...r, id: crypto.randomUUID() }));
    return db.insert(importedTransactions).values(withIds).returning();
  }

  async getImportedTransactionsByBusiness(businessId: string): Promise<ImportedTransaction[]> {
    return db
      .select()
      .from(importedTransactions)
      .where(eq(importedTransactions.businessId, businessId))
      .orderBy(desc(importedTransactions.txnDate), desc(importedTransactions.createdAt));
  }

  async getImportBatchesByBusiness(businessId: string): Promise<Array<{ importBatchId: string; count: number; sourceFileName: string | null; importedByName: string | null; createdAt: Date }>> {
    const rows = await db
      .select({
        importBatchId: importedTransactions.importBatchId,
        count: sql<number>`COUNT(*)::int`,
        sourceFileName: sql<string | null>`MAX(${importedTransactions.sourceFileName})`,
        importedByName: sql<string | null>`MAX(${importedTransactions.importedByName})`,
        createdAt: sql<Date>`MIN(${importedTransactions.createdAt})`,
      })
      .from(importedTransactions)
      .where(eq(importedTransactions.businessId, businessId))
      .groupBy(importedTransactions.importBatchId)
      .orderBy(desc(sql`MIN(${importedTransactions.createdAt})`));
    return rows;
  }

  async deleteImportBatch(importBatchId: string, businessId: string): Promise<void> {
    await db
      .delete(importedTransactions)
      .where(and(eq(importedTransactions.importBatchId, importBatchId), eq(importedTransactions.businessId, businessId)));
  }

  // ──────────── Demo requests (public marketing form) ────────────
  async createDemoRequest(row: InsertDemoRequest): Promise<DemoRequest> {
    const [result] = await db.insert(demoRequests).values({ ...row, id: crypto.randomUUID() }).returning();
    return result;
  }

  async getDemoRequestsPaginated(limit: number, offset: number): Promise<{ data: DemoRequest[]; total: number }> {
    const safeLimit = Math.min(200, Math.max(1, limit));
    const safeOffset = Math.max(0, offset);
    const [countRow] = await db.select({ count: sql<number>`COUNT(*)::int` }).from(demoRequests);
    const data = await db
      .select()
      .from(demoRequests)
      .orderBy(desc(demoRequests.createdAt))
      .limit(safeLimit)
      .offset(safeOffset);
    return { data, total: countRow?.count ?? 0 };
  }

  async updateDemoRequestStatus(id: string, status: string): Promise<DemoRequest | undefined> {
    const [result] = await db.update(demoRequests).set({ status }).where(eq(demoRequests.id, id)).returning();
    return result;
  }

  // ──────────── Admin Analytics ────────────
  async getCompaniesOverview(): Promise<any> {
    const [[totalBiz], kycBreakdown, subscriptionsByPlan, subscriptionsByStatus, recentSignups, monthlyGrowth] = await Promise.all([
      db.select({ count: sql<number>`COUNT(*)::int` }).from(businesses).where(eq(businesses.isSandbox, false)),
      db.select({ status: businesses.kycStatus, count: sql<number>`COUNT(*)::int` }).from(businesses).where(eq(businesses.isSandbox, false)).groupBy(businesses.kycStatus),
      db.select({ plan: subscriptions.plan, count: sql<number>`COUNT(*)::int` }).from(subscriptions).where(sql`${subscriptions.businessId} NOT LIKE 'demo-%'`).groupBy(subscriptions.plan),
      db.select({ status: subscriptions.status, count: sql<number>`COUNT(*)::int` }).from(subscriptions).where(sql`${subscriptions.businessId} NOT LIKE 'demo-%'`).groupBy(subscriptions.status),
      db.select().from(businesses).where(eq(businesses.isSandbox, false)).orderBy(desc(businesses.createdAt)).limit(5),
      db.select({ month: sql<string>`TO_CHAR(${businesses.createdAt}, 'YYYY-MM')`, count: sql<number>`COUNT(*)::int` })
        .from(businesses)
        .where(sql`${businesses.createdAt} >= NOW() - INTERVAL '6 months' AND ${businesses.isSandbox} = false`)
        .groupBy(sql`TO_CHAR(${businesses.createdAt}, 'YYYY-MM')`)
        .orderBy(sql`TO_CHAR(${businesses.createdAt}, 'YYYY-MM')`),
    ]);

    return {
      totalBusinesses: totalBiz?.count || 0,
      kycBreakdown,
      subscriptionsByPlan,
      subscriptionsByStatus,
      recentSignups,
      monthlyGrowth,
    };
  }

  async getPlatformWalletSummary(page: number = 1, limit: number = 50, type?: string): Promise<any> {
    // Total approved topups (inflows)
    const [topupTotals] = await db.select({
      total: sql<string>`COALESCE(SUM(${walletTopups.amount}::numeric), 0)::text`,
      count: sql<number>`COUNT(*)::int`,
    }).from(walletTopups).where(eq(walletTopups.status, "approved"));

    // Total completed disbursements (outflows) - amount + fee
    const [disburseTotals] = await db.select({
      total: sql<string>`COALESCE(SUM(COALESCE(${bulkPaymentItems.amount}::numeric, 0) + COALESCE(${bulkPaymentItems.fee}::numeric, 0)), 0)::text`,
      count: sql<number>`COUNT(*)::int`,
    }).from(bulkPaymentItems).where(eq(bulkPaymentItems.status, "completed" as any));

    const totalIn = Number(topupTotals?.total || "0");
    const totalOut = Number(disburseTotals?.total || "0");
    const balance = totalIn - totalOut;

    // Build unified ledger entries
    const offset = (page - 1) * limit;

    // Topup rows — all fields for super-admin detail view
    const topupRows = await db.select({
      id: walletTopups.id,
      createdAt: walletTopups.createdAt,
      amount: walletTopups.amount,
      method: walletTopups.method,
      reference: walletTopups.reference,
      userName: walletTopups.userName,
      userId: walletTopups.userId,
      phone: walletTopups.phone,
      mpesaReceiptNumber: walletTopups.mpesaReceiptNumber,
      merchantRequestId: walletTopups.merchantRequestId,
      checkoutRequestId: walletTopups.checkoutRequestId,
      checkoutResultCode: walletTopups.checkoutResultCode,
      checkoutResultDesc: walletTopups.checkoutResultDesc,
      popFileName: walletTopups.popFileName,
      reviewerName: walletTopups.reviewerName,
      reviewNote: walletTopups.reviewNote,
      reviewedAt: walletTopups.reviewedAt,
      status: walletTopups.status,
      paymentInitiatedAt: walletTopups.paymentInitiatedAt,
    }).from(walletTopups).where(eq(walletTopups.status, "approved")).orderBy(desc(walletTopups.createdAt));

    // Disbursement rows — all fields for super-admin detail view
    const disburseRows = await db.select({
      id: bulkPaymentItems.id,
      createdAt: bulkPaymentItems.processedAt,
      amount: bulkPaymentItems.amount,
      fee: bulkPaymentItems.fee,
      recipient: bulkPaymentItems.recipient,
      accountNumber: bulkPaymentItems.accountNumber,
      reference: bulkPaymentItems.reference,
      systemRef: bulkPaymentItems.systemRef,
      mpesaTransactionId: bulkPaymentItems.mpesaTransactionId,
      batchId: bulkPaymentItems.batchId,
      recipientNameFromMpesa: bulkPaymentItems.recipientNameFromMpesa,
      expenseCategory: bulkPaymentItems.expenseCategory,
      failureReason: bulkPaymentItems.failureReason,
    }).from(bulkPaymentItems).where(eq(bulkPaymentItems.status, "completed" as any)).orderBy(desc(bulkPaymentItems.processedAt));

    // Merge and sort by date desc
    const credits = topupRows.map((t) => ({
      id: `topup-${t.id}`,
      rawId: t.id,
      type: t.mpesaReceiptNumber ? "stk_push" : (t.method === "bank_transfer" ? "bank_topup" : t.method),
      direction: "credit" as const,
      amount: Number(t.amount),
      label: t.userName || t.userId,
      reference: t.reference,
      receipt: t.mpesaReceiptNumber,
      createdAt: t.createdAt?.toISOString() ?? new Date().toISOString(),
      // Full detail fields
      detail: {
        userId: t.userId,
        userName: t.userName,
        phone: t.phone,
        method: t.method,
        reference: t.reference,
        mpesaReceiptNumber: t.mpesaReceiptNumber,
        merchantRequestId: t.merchantRequestId,
        checkoutRequestId: t.checkoutRequestId,
        checkoutResultCode: t.checkoutResultCode,
        checkoutResultDesc: t.checkoutResultDesc,
        popFileName: t.popFileName,
        reviewerName: t.reviewerName,
        reviewNote: t.reviewNote,
        reviewedAt: t.reviewedAt?.toISOString() ?? null,
        paymentInitiatedAt: t.paymentInitiatedAt?.toISOString() ?? null,
        status: t.status,
      },
    }));

    const debits = disburseRows.map((d) => ({
      id: `disburse-${d.id}`,
      rawId: d.id,
      type: "disbursement",
      direction: "debit" as const,
      amount: Number(d.amount) + Number(d.fee || "0"),
      label: d.recipient,
      reference: d.systemRef || d.reference || d.batchId || "",
      receipt: d.mpesaTransactionId,
      createdAt: (d.createdAt ?? new Date()).toISOString(),
      // Full detail fields
      detail: {
        recipient: d.recipient,
        accountNumber: d.accountNumber,
        recipientNameFromMpesa: d.recipientNameFromMpesa,
        principalAmount: Number(d.amount),
        fee: Number(d.fee || "0"),
        totalDeducted: Number(d.amount) + Number(d.fee || "0"),
        reference: d.reference,
        systemRef: d.systemRef,
        batchId: d.batchId,
        mpesaTransactionId: d.mpesaTransactionId,
        expenseCategory: d.expenseCategory,
      },
    }));

    let all = [...credits, ...debits].sort(
      (a, b) => new Date(b.createdAt).getTime() - new Date(a.createdAt).getTime()
    );

    if (type && type !== "all") {
      all = all.filter((e) => type === "credit" ? e.direction === "credit" : type === "debit" ? e.direction === "debit" : e.type === type);
    }

    const total = all.length;
    const entries = all.slice(offset, offset + limit);

    return {
      balance: balance.toFixed(2),
      totalIn: totalIn.toFixed(2),
      totalOut: totalOut.toFixed(2),
      topupCount: topupTotals?.count ?? 0,
      disburseCount: disburseTotals?.count ?? 0,
      total,
      page,
      limit,
      entries,
    };
  }

  async getWalletsOverview(): Promise<any> {
    const [totalVolume] = await db.select({
      total: sql<string>`COALESCE(SUM(${transactions.amount}::numeric), 0)::text`,
    }).from(transactions);

    const dailyVolume = await db.select({
      date: sql<string>`TO_CHAR(${transactions.createdAt}, 'YYYY-MM-DD')`,
      volume: sql<string>`COALESCE(SUM(${transactions.amount}::numeric), 0)::text`,
      count: sql<number>`COUNT(*)::int`,
    }).from(transactions)
      .where(sql`${transactions.createdAt} >= NOW() - INTERVAL '30 days'`)
      .groupBy(sql`TO_CHAR(${transactions.createdAt}, 'YYYY-MM-DD')`)
      .orderBy(sql`TO_CHAR(${transactions.createdAt}, 'YYYY-MM-DD')`);

    const weeklyVolume = await db.select({
      week: sql<string>`TO_CHAR(DATE_TRUNC('week', ${transactions.createdAt}), 'YYYY-MM-DD')`,
      volume: sql<string>`COALESCE(SUM(${transactions.amount}::numeric), 0)::text`,
      count: sql<number>`COUNT(*)::int`,
    }).from(transactions)
      .where(sql`${transactions.createdAt} >= NOW() - INTERVAL '12 weeks'`)
      .groupBy(sql`DATE_TRUNC('week', ${transactions.createdAt})`)
      .orderBy(sql`DATE_TRUNC('week', ${transactions.createdAt})`);

    const monthlyVolume = await db.select({
      month: sql<string>`TO_CHAR(${transactions.createdAt}, 'YYYY-MM')`,
      volume: sql<string>`COALESCE(SUM(${transactions.amount}::numeric), 0)::text`,
      count: sql<number>`COUNT(*)::int`,
    }).from(transactions)
      .where(sql`${transactions.createdAt} >= NOW() - INTERVAL '12 months'`)
      .groupBy(sql`TO_CHAR(${transactions.createdAt}, 'YYYY-MM')`)
      .orderBy(sql`TO_CHAR(${transactions.createdAt}, 'YYYY-MM')`);

    const paymentTypeBreakdown = await db.select({
      type: transactions.type,
      volume: sql<string>`COALESCE(SUM(${transactions.amount}::numeric), 0)::text`,
      count: sql<number>`COUNT(*)::int`,
    }).from(transactions).groupBy(transactions.type);

    return {
      totalVolume: totalVolume?.total || "0",
      dailyVolume,
      weeklyVolume,
      monthlyVolume,
      paymentTypeBreakdown,
    };
  }

  async getPaymentsOverview(): Promise<any> {
    // Super-admin "All Payments" must exclude sandbox/demo-company data.
    const nonSandboxBatchWhere = sql`
      ${bulkPaymentBatches.createdByUserId} IS NOT NULL
      AND ${bulkPaymentBatches.createdByUserId} IN (
        SELECT users.id
        FROM users
        LEFT JOIN businesses ON businesses.id = users.business_id
        WHERE COALESCE(businesses.is_sandbox, false) = false
      )
    `;
    const nonSandboxApprovalWhere = sql`
      ${approvals.makerId} IN (
        SELECT users.id
        FROM users
        LEFT JOIN businesses ON businesses.id = users.business_id
        WHERE COALESCE(businesses.is_sandbox, false) = false
      )
    `;
    const nonSandboxLinkWhere = sql`
      ${paymentLinks.createdByUserId} IS NOT NULL
      AND ${paymentLinks.createdByUserId} IN (
        SELECT users.id
        FROM users
        LEFT JOIN businesses ON businesses.id = users.business_id
        WHERE COALESCE(businesses.is_sandbox, false) = false
      )
    `;

    // Run all 9 queries in parallel for maximum speed
    const [
      [batchStats],
      [approvalStats],
      [batchTxnStats],
      [linkTxnStats],
      byPaymentType,
      monthlyPayments,
      [linkStats],
      recentBatches,
      recentApprovals,
    ] = await Promise.all([
      db.select({
        totalBatches: sql`COUNT(*)::int`,
        totalAmount: sql`COALESCE(SUM(${bulkPaymentBatches.totalAmount}::numeric), 0)::text`,
        pendingBatches: sql`COUNT(*) FILTER (WHERE ${bulkPaymentBatches.status} IN ('pending', 'processing'))::int`,
        completedBatches: sql`COUNT(*) FILTER (WHERE ${bulkPaymentBatches.status} = 'completed')::int`,
        failedBatches: sql`COUNT(*) FILTER (WHERE ${bulkPaymentBatches.status} = 'failed')::int`,
      }).from(bulkPaymentBatches).where(nonSandboxBatchWhere),

      db.select({
        totalApprovals: sql`COUNT(*)::int`,
        pendingChecker: sql`COUNT(*) FILTER (WHERE ${approvals.status} = 'pending_checker')::int`,
        pendingApprover: sql`COUNT(*) FILTER (WHERE ${approvals.status} = 'pending_approver')::int`,
        approved: sql`COUNT(*) FILTER (WHERE ${approvals.status} = 'approved')::int`,
        rejected: sql`COUNT(*) FILTER (WHERE ${approvals.status} = 'rejected')::int`,
      }).from(approvals).where(nonSandboxApprovalWhere),

      // Build transaction summary from traceable payment records only.
      db.select({
        totalCount: sql`COUNT(*)::int`,
        totalVolume: sql`COALESCE(SUM(${bulkPaymentBatches.totalAmount}::numeric), 0)::text`,
        completedCount: sql`COUNT(*) FILTER (WHERE ${bulkPaymentBatches.status} = 'completed')::int`,
        pendingCount: sql`COUNT(*) FILTER (WHERE ${bulkPaymentBatches.status} IN ('pending', 'processing'))::int`,
        failedCount: sql`COUNT(*) FILTER (WHERE ${bulkPaymentBatches.status} = 'failed')::int`,
      }).from(bulkPaymentBatches).where(nonSandboxBatchWhere),

      db.select({
        totalCount: sql`COUNT(*) FILTER (WHERE ${paymentLinks.status} = 'paid')::int`,
        totalVolume: sql`COALESCE(SUM(${paymentLinks.paidAmount}::numeric), 0)::text`,
        completedCount: sql`COUNT(*) FILTER (WHERE ${paymentLinks.status} = 'paid')::int`,
        pendingCount: sql`COUNT(*) FILTER (WHERE ${paymentLinks.status} IN ('active', 'payment_pending'))::int`,
        failedCount: sql`COUNT(*) FILTER (WHERE ${paymentLinks.status} IN ('failed', 'expired'))::int`,
      }).from(paymentLinks).where(nonSandboxLinkWhere),

      db.select({
        type: bulkPaymentBatches.paymentType,
        count: sql`COUNT(*)::int`,
        volume: sql`COALESCE(SUM(${bulkPaymentBatches.totalAmount}::numeric), 0)::text`,
      }).from(bulkPaymentBatches).where(nonSandboxBatchWhere).groupBy(bulkPaymentBatches.paymentType),

      db.select({
        month: sql`TO_CHAR(${bulkPaymentBatches.createdAt}, 'Mon')`,
        count: sql`COUNT(*)::int`,
        volume: sql`COALESCE(SUM(${bulkPaymentBatches.totalAmount}::numeric), 0)::text`,
      }).from(bulkPaymentBatches)
        .where(sql`${nonSandboxBatchWhere} AND ${bulkPaymentBatches.createdAt} >= NOW() - INTERVAL '6 months'`)
        .groupBy(sql`TO_CHAR(${bulkPaymentBatches.createdAt}, 'Mon'), DATE_TRUNC('month', ${bulkPaymentBatches.createdAt})`)
        .orderBy(sql`DATE_TRUNC('month', ${bulkPaymentBatches.createdAt})`),

      db.select({
        totalLinks: sql`COUNT(*)::int`,
        activeLinks: sql`COUNT(*) FILTER (WHERE ${paymentLinks.status} = 'active')::int`,
        paidLinks: sql`COUNT(*) FILTER (WHERE ${paymentLinks.status} = 'paid')::int`,
        totalLinksAmount: sql`COALESCE(SUM(${paymentLinks.amount}::numeric), 0)::text`,
      }).from(paymentLinks).where(nonSandboxLinkWhere),

      db.select({
        id: bulkPaymentBatches.id,
        paymentType: bulkPaymentBatches.paymentType,
        status: bulkPaymentBatches.status,
        totalAmount: bulkPaymentBatches.totalAmount,
        recipientCount: bulkPaymentBatches.recipientCount,
        completedCount: bulkPaymentBatches.completedCount,
        failedCount: bulkPaymentBatches.failedCount,
        processedAt: bulkPaymentBatches.processedAt,
        createdAt: bulkPaymentBatches.createdAt,
        createdByName: sql`COALESCE(${bulkPaymentBatches.createdByName}, ${users.fullName}, ${users.email}, 'Unknown')`,
      }).from(bulkPaymentBatches)
        .leftJoin(users, eq(bulkPaymentBatches.createdByUserId, users.id))
        .where(nonSandboxBatchWhere)
        .orderBy(sql`${bulkPaymentBatches.createdAt} DESC`)
        .limit(5),

      db.select().from(approvals)
        .where(nonSandboxApprovalWhere)
        .orderBy(sql`${approvals.createdAt} DESC`)
        .limit(5),
    ]);

    const bCount = Number(batchTxnStats?.totalCount || 0);
    const lCount = Number(linkTxnStats?.totalCount || 0);
    const bVol = Number(batchTxnStats?.totalVolume || 0);
    const lVol = Number(linkTxnStats?.totalVolume || 0);
    const txnStats = {
      totalCount: bCount + lCount,
      totalVolume: String(bVol + lVol),
      completedCount: Number(batchTxnStats?.completedCount || 0) + Number(linkTxnStats?.completedCount || 0),
      pendingCount: Number(batchTxnStats?.pendingCount || 0) + Number(linkTxnStats?.pendingCount || 0),
      failedCount: Number(batchTxnStats?.failedCount || 0) + Number(linkTxnStats?.failedCount || 0),
    };

    return {
      batches: batchStats,
      approvals: approvalStats,
      transactions: txnStats,
      byPaymentType,
      monthlyPayments,
      paymentLinks: linkStats,
      recentBatches,
      recentApprovals,
    };
  }

  async getSubscriptionsOverview(): Promise<any> {
    const byPlan = await db.select({
      plan: subscriptions.plan,
      count: sql<number>`COUNT(*)::int`,
      totalAmount: sql<string>`COALESCE(SUM(${subscriptions.amount}::numeric), 0)::text`,
    }).from(subscriptions).where(sql`${subscriptions.businessId} NOT LIKE 'demo-%'`).groupBy(subscriptions.plan);

    const byStatus = await db.select({
      status: subscriptions.status,
      count: sql<number>`COUNT(*)::int`,
    }).from(subscriptions).where(sql`${subscriptions.businessId} NOT LIKE 'demo-%'`).groupBy(subscriptions.status);

    const [mrr] = await db.select({
      total: sql<string>`COALESCE(SUM(${subscriptions.amount}::numeric), 0)::text`,
    }).from(subscriptions).where(and(eq(subscriptions.status, "active"), sql`${subscriptions.businessId} NOT LIKE 'demo-%'`));

    const [totalRevenue] = await db.select({
      total: sql<string>`COALESCE(SUM(${invoices.totalAmount}::numeric), 0)::text`,
    }).from(invoices).where(and(eq(invoices.status, "paid"), sql`${invoices.businessId} NOT LIKE 'demo-%'`));

    const [activeCount] = await db.select({ count: sql<number>`COUNT(*)::int` }).from(subscriptions).where(and(eq(subscriptions.status, "active"), sql`${subscriptions.businessId} NOT LIKE 'demo-%'`));
    const [cancelledCount] = await db.select({ count: sql<number>`COUNT(*)::int` }).from(subscriptions).where(and(eq(subscriptions.status, "cancelled"), sql`${subscriptions.businessId} NOT LIKE 'demo-%'`));
    const [trialCount] = await db.select({ count: sql<number>`COUNT(*)::int` }).from(subscriptions).where(and(eq(subscriptions.status, "trial"), sql`${subscriptions.businessId} NOT LIKE 'demo-%'`));
    const [pastDueCount] = await db.select({ count: sql<number>`COUNT(*)::int` }).from(subscriptions).where(and(eq(subscriptions.status, "past_due"), sql`${subscriptions.businessId} NOT LIKE 'demo-%'`));
    const [paidInvoices] = await db.select({ count: sql<number>`COUNT(*)::int` }).from(invoices).where(and(eq(invoices.status, "paid"), sql`${invoices.businessId} NOT LIKE 'demo-%'`));
    const [overdueInvoices] = await db.select({ count: sql<number>`COUNT(*)::int` }).from(invoices).where(and(eq(invoices.status, "overdue"), sql`${invoices.businessId} NOT LIKE 'demo-%'`));

    const totalSubscriptions = (activeCount?.count || 0)
      + (cancelledCount?.count || 0)
      + (trialCount?.count || 0)
      + (pastDueCount?.count || 0);

    return {
      totalSubscriptions,
      activeCount: activeCount?.count || 0,
      pastDueCount: pastDueCount?.count || 0,
      cancelledCount: cancelledCount?.count || 0,
      trialCount: trialCount?.count || 0,
      byPlan,
      mrr: mrr?.total || "0",
      totalRevenue: totalRevenue?.total || "0",
      paidInvoices: paidInvoices?.count || 0,
      overdueInvoices: overdueInvoices?.count || 0,
      byStatus,
      monthlyRecurringRevenue: mrr?.total || "0",
      activeSubscriptions: activeCount?.count || 0,
      cancelledSubscriptions: cancelledCount?.count || 0,
    };
  }

  async getTicketsOverview(): Promise<any> {
    const nonDemoRealTicketFilter = sql`
      COALESCE(${supportTickets.businessId}, '') NOT LIKE 'demo-%'
      AND (${supportTickets.businessId} IS NOT NULL OR ${supportTickets.userId} IS NOT NULL)
    `;
    const byStatus = await db.select({
      status: supportTickets.status,
      count: sql<number>`COUNT(*)::int`,
    }).from(supportTickets).where(nonDemoRealTicketFilter).groupBy(supportTickets.status);

    const byPriority = await db.select({
      priority: supportTickets.priority,
      count: sql<number>`COUNT(*)::int`,
    }).from(supportTickets).where(nonDemoRealTicketFilter).groupBy(supportTickets.priority);

    const [avgResolution] = await db.select({
      avgHours: sql<string>`COALESCE(ROUND(AVG(EXTRACT(EPOCH FROM (${supportTickets.resolvedAt} - ${supportTickets.createdAt})) / 3600)::numeric, 1), 0)::text`,
    }).from(supportTickets).where(and(sql`${supportTickets.resolvedAt} IS NOT NULL`, nonDemoRealTicketFilter));

    const [totalTickets] = await db.select({ count: sql<number>`COUNT(*)::int` }).from(supportTickets).where(nonDemoRealTicketFilter);

    return {
      totalTickets: totalTickets?.count || 0,
      byStatus,
      byPriority,
      averageResolutionHours: avgResolution?.avgHours || "0",
    };
  }

  async getAuditTrails(limit = 100): Promise<any> {
    const auditLogs = await db.select({
      id: mpesaAuditLogs.id,
      type: mpesaAuditLogs.type,
      payload: mpesaAuditLogs.payload,
      createdAt: mpesaAuditLogs.createdAt,
    }).from(mpesaAuditLogs)
      .orderBy(sql`${mpesaAuditLogs.createdAt} DESC`)
      .limit(limit);

    const approvalLogs = await db.select({
      id: approvals.id,
      entityId: approvals.entityId,
      status: approvals.status,
      makerName: approvals.makerName,
      checkerName: approvals.checkerName,
      approverName: approvals.approverName,
      createdAt: approvals.createdAt,
      checkerActionAt: approvals.checkerActionAt,
      approverActionAt: approvals.approverActionAt,
    }).from(approvals)
      .orderBy(sql`${approvals.createdAt} DESC`)
      .limit(limit);

    const batchLogs = await db.select({
      id: bulkPaymentBatches.id,
      status: bulkPaymentBatches.status,
      paymentType: bulkPaymentBatches.paymentType,
      totalAmount: bulkPaymentBatches.totalAmount,
      recipientCount: bulkPaymentBatches.recipientCount,
      completedCount: bulkPaymentBatches.completedCount,
      failedCount: bulkPaymentBatches.failedCount,
      createdAt: bulkPaymentBatches.createdAt,
      processedAt: bulkPaymentBatches.processedAt,
      createdByName: sql<string>`COALESCE(${bulkPaymentBatches.createdByName}, (SELECT full_name FROM users WHERE id = ${bulkPaymentBatches.createdByUserId}), 'System')`,
    }).from(bulkPaymentBatches)
      .orderBy(sql`${bulkPaymentBatches.createdAt} DESC`)
      .limit(limit);

    const events: any[] = [];

    for (const log of auditLogs) {
      events.push({
        id: log.id,
        category: "mpesa",
        type: log.type,
        description: `M-Pesa ${log.type} event`,
        details: log.payload,
        createdAt: log.createdAt,
      });
    }

    for (const a of approvalLogs) {
      const actions: any[] = [];
      actions.push({ action: "Created", by: a.makerName || "Unknown", at: a.createdAt });
      if (a.checkerActionAt) {
        actions.push({ action: a.status === "rejected" ? "Rejected" : "Checked", by: a.checkerName || "Unknown", at: a.checkerActionAt });
      }
      if (a.approverActionAt) {
        actions.push({ action: "Approved", by: a.approverName || "Unknown", at: a.approverActionAt });
      }
      events.push({
        id: a.id,
        category: "approval",
        type: `Approval ${a.status}`,
        description: `Payment approval workflow — ${a.status.replace(/_/g, " ")}`,
        status: a.status,
        actions,
        createdAt: a.createdAt,
      });
    }

    for (const b of batchLogs) {
      events.push({
        id: b.id,
        category: "batch",
        type: `Batch ${b.status}`,
        description: `Batch ${b.paymentType} — ${b.recipientCount} recipients, KES ${b.totalAmount}`,
        status: b.status,
        paymentType: b.paymentType,
        completedCount: b.completedCount,
        failedCount: b.failedCount,
        createdByName: b.createdByName,
        createdAt: b.createdAt,
        processedAt: b.processedAt,
      });
    }

    events.sort((a, b) => new Date(b.createdAt).getTime() - new Date(a.createdAt).getTime());

    const [mpesaCount] = await db.select({ count: sql<number>`COUNT(*)::int` }).from(mpesaAuditLogs);
    const [approvalCount] = await db.select({ count: sql<number>`COUNT(*)::int` }).from(approvals);
    const [batchCount] = await db.select({ count: sql<number>`COUNT(*)::int` }).from(bulkPaymentBatches);
    const [todayMpesa] = await db.select({ count: sql<number>`COUNT(*)::int` }).from(mpesaAuditLogs)
      .where(sql`${mpesaAuditLogs.createdAt} >= CURRENT_DATE`);
    const [todayApprovals] = await db.select({ count: sql<number>`COUNT(*)::int` }).from(approvals)
      .where(sql`${approvals.createdAt} >= CURRENT_DATE`);
    const [todayBatches] = await db.select({ count: sql<number>`COUNT(*)::int` }).from(bulkPaymentBatches)
      .where(sql`${bulkPaymentBatches.createdAt} >= CURRENT_DATE`);

    return {
      events,
      stats: {
        totalEvents: (mpesaCount?.count || 0) + (approvalCount?.count || 0) + (batchCount?.count || 0),
        mpesaEvents: mpesaCount?.count || 0,
        approvalEvents: approvalCount?.count || 0,
        batchEvents: batchCount?.count || 0,
        todayEvents: (todayMpesa?.count || 0) + (todayApprovals?.count || 0) + (todayBatches?.count || 0),
      },
    };
  }

  async getDailyReconciliation(date?: string): Promise<any> {
    const targetDate = date || new Date().toISOString().split("T")[0];

    const [inbound] = await db.select({
      count: sql<number>`COUNT(*)::int`,
      volume: sql<string>`COALESCE(SUM(amount::numeric), 0)::text`,
    }).from(transactions)
      .where(sql`type = 'in' AND DATE(${transactions.createdAt}) = ${targetDate}`);

    const [outbound] = await db.select({
      count: sql<number>`COUNT(*)::int`,
      volume: sql<string>`COALESCE(SUM(amount::numeric), 0)::text`,
    }).from(transactions)
      .where(sql`type = 'out' AND DATE(${transactions.createdAt}) = ${targetDate}`);

    const [batchSummary] = await db.select({
      totalBatches: sql<number>`COUNT(*)::int`,
      completedBatches: sql<number>`COUNT(*) FILTER (WHERE status = 'completed')::int`,
      failedBatches: sql<number>`COUNT(*) FILTER (WHERE status = 'failed')::int`,
      processingBatches: sql<number>`COUNT(*) FILTER (WHERE status = 'processing')::int`,
      pendingBatches: sql<number>`COUNT(*) FILTER (WHERE status = 'pending')::int`,
      totalAmount: sql<string>`COALESCE(SUM(${bulkPaymentBatches.totalAmount}::numeric), 0)::text`,
      successAmount: sql<string>`COALESCE(SUM(CASE WHEN status = 'completed' THEN ${bulkPaymentBatches.totalAmount}::numeric ELSE 0 END), 0)::text`,
    }).from(bulkPaymentBatches)
      .where(sql`DATE(${bulkPaymentBatches.createdAt}) = ${targetDate}`);

    const [itemSummary] = await db.select({
      totalItems: sql<number>`COUNT(*)::int`,
      successItems: sql<number>`COUNT(*) FILTER (WHERE ${bulkPaymentItems.status} = 'completed')::int`,
      failedItems: sql<number>`COUNT(*) FILTER (WHERE ${bulkPaymentItems.status} = 'failed')::int`,
      pendingItems: sql<number>`COUNT(*) FILTER (WHERE ${bulkPaymentItems.status} = 'pending')::int`,
      totalAmount: sql<string>`COALESCE(SUM(${bulkPaymentItems.amount}::numeric), 0)::text`,
      successAmount: sql<string>`COALESCE(SUM(CASE WHEN ${bulkPaymentItems.status} = 'completed' THEN ${bulkPaymentItems.amount}::numeric ELSE 0 END), 0)::text`,
    }).from(bulkPaymentItems)
      .innerJoin(bulkPaymentBatches, eq(bulkPaymentItems.batchId, bulkPaymentBatches.id))
      .where(sql`DATE(${bulkPaymentBatches.createdAt}) = ${targetDate}`);

    const dailyTrend = await db.select({
      date: sql<string>`DATE(${transactions.createdAt})::text`,
      inCount: sql<number>`COUNT(*) FILTER (WHERE type = 'in')::int`,
      outCount: sql<number>`COUNT(*) FILTER (WHERE type = 'out')::int`,
      inVolume: sql<string>`COALESCE(SUM(CASE WHEN type = 'in' THEN amount::numeric ELSE 0 END), 0)::text`,
      outVolume: sql<string>`COALESCE(SUM(CASE WHEN type = 'out' THEN amount::numeric ELSE 0 END), 0)::text`,
    }).from(transactions)
      .where(sql`${transactions.createdAt} >= CURRENT_DATE - INTERVAL '14 days'`)
      .groupBy(sql`DATE(${transactions.createdAt})`)
      .orderBy(sql`DATE(${transactions.createdAt})`);

    const failedItems = await db.select({
      id: bulkPaymentItems.id,
      recipient: bulkPaymentItems.recipient,
      amount: bulkPaymentItems.amount,
      status: bulkPaymentItems.status,
      failureReason: bulkPaymentItems.failureReason,
      reference: bulkPaymentItems.reference,
      batchId: bulkPaymentItems.batchId,
      processedAt: bulkPaymentItems.processedAt,
    }).from(bulkPaymentItems)
      .innerJoin(bulkPaymentBatches, eq(bulkPaymentItems.batchId, bulkPaymentBatches.id))
      .where(sql`${bulkPaymentItems.status} = 'failed' AND DATE(${bulkPaymentBatches.createdAt}) = ${targetDate}`)
      .limit(50);

    const net = parseFloat(inbound?.volume || "0") - parseFloat(outbound?.volume || "0");

    return {
      date: targetDate,
      inbound: { count: inbound?.count || 0, volume: inbound?.volume || "0" },
      outbound: { count: outbound?.count || 0, volume: outbound?.volume || "0" },
      netPosition: net.toFixed(2),
      batches: batchSummary || {},
      items: itemSummary || {},
      dailyTrend,
      failedItems,
    };
  }

  async getReportsData(type: string, startDate?: string, endDate?: string): Promise<any> {
    const dateFilter = (col: any) => {
      const conditions = [];
      if (startDate) conditions.push(sql`${col} >= ${startDate}::date`);
      if (endDate) conditions.push(sql`${col} <= ${endDate}::date + INTERVAL '1 day'`);
      return conditions.length > 0 ? and(...conditions) : undefined;
    };

    if (type === "payments") {
      const byType = await db.select({
        type: bulkPaymentBatches.paymentType,
        count: sql<number>`COUNT(*)::int`,
        volume: sql<string>`COALESCE(SUM(${bulkPaymentBatches.totalAmount}::numeric), 0)::text`,
        successCount: sql<number>`COUNT(*) FILTER (WHERE status = 'completed')::int`,
        failedCount: sql<number>`COUNT(*) FILTER (WHERE status = 'failed')::int`,
      }).from(bulkPaymentBatches)
        .where(dateFilter(bulkPaymentBatches.createdAt))
        .groupBy(bulkPaymentBatches.paymentType);

      const monthly = await db.select({
        month: sql<string>`TO_CHAR(${bulkPaymentBatches.createdAt}, 'YYYY-MM')`,
        monthLabel: sql<string>`TO_CHAR(${bulkPaymentBatches.createdAt}, 'Mon YYYY')`,
        count: sql<number>`COUNT(*)::int`,
        volume: sql<string>`COALESCE(SUM(${bulkPaymentBatches.totalAmount}::numeric), 0)::text`,
        successRate: sql<string>`ROUND(COUNT(*) FILTER (WHERE status = 'completed') * 100.0 / NULLIF(COUNT(*), 0), 1)::text`,
      }).from(bulkPaymentBatches)
        .where(dateFilter(bulkPaymentBatches.createdAt))
        .groupBy(sql`TO_CHAR(${bulkPaymentBatches.createdAt}, 'YYYY-MM'), TO_CHAR(${bulkPaymentBatches.createdAt}, 'Mon YYYY')`)
        .orderBy(sql`TO_CHAR(${bulkPaymentBatches.createdAt}, 'YYYY-MM')`);

      const [totals] = await db.select({
        totalBatches: sql<number>`COUNT(*)::int`,
        totalVolume: sql<string>`COALESCE(SUM(${bulkPaymentBatches.totalAmount}::numeric), 0)::text`,
        totalRecipients: sql<number>`COALESCE(SUM(${bulkPaymentBatches.recipientCount}), 0)::int`,
        avgBatchSize: sql<string>`ROUND(AVG(${bulkPaymentBatches.recipientCount}), 1)::text`,
      }).from(bulkPaymentBatches).where(dateFilter(bulkPaymentBatches.createdAt));

      return { byType, monthly, totals };
    }

    if (type === "loans") {
      const byStatus = await db.select({
        status: cashAdvances.status,
        count: sql<number>`COUNT(*)::int`,
        volume: sql<string>`COALESCE(SUM(${cashAdvances.amount}::numeric), 0)::text`,
      }).from(cashAdvances)
        .where(dateFilter(cashAdvances.createdAt))
        .groupBy(cashAdvances.status);

      const monthly = await db.select({
        month: sql<string>`TO_CHAR(${cashAdvances.createdAt}, 'YYYY-MM')`,
        monthLabel: sql<string>`TO_CHAR(${cashAdvances.createdAt}, 'Mon YYYY')`,
        count: sql<number>`COUNT(*)::int`,
        disbursed: sql<string>`COALESCE(SUM(CASE WHEN status IN ('active', 'repaid') THEN ${cashAdvances.amount}::numeric ELSE 0 END), 0)::text`,
        repaid: sql<string>`COALESCE(SUM(${cashAdvances.totalRepayment}::numeric), 0)::text`,
      }).from(cashAdvances)
        .where(dateFilter(cashAdvances.createdAt))
        .groupBy(sql`TO_CHAR(${cashAdvances.createdAt}, 'YYYY-MM'), TO_CHAR(${cashAdvances.createdAt}, 'Mon YYYY')`)
        .orderBy(sql`TO_CHAR(${cashAdvances.createdAt}, 'YYYY-MM')`);

      const [totals] = await db.select({
        totalAdvances: sql<number>`COUNT(*)::int`,
        totalDisbursed: sql<string>`COALESCE(SUM(CASE WHEN status IN ('active', 'repaid') THEN ${cashAdvances.amount}::numeric ELSE 0 END), 0)::text`,
        totalRepaid: sql<string>`COALESCE(SUM(${cashAdvances.totalRepayment}::numeric), 0)::text`,
        avgInterestRate: sql<string>`ROUND(AVG(${cashAdvances.interestRate}::numeric), 2)::text`,
        activeCount: sql<number>`COUNT(*) FILTER (WHERE status = 'active')::int`,
      }).from(cashAdvances).where(dateFilter(cashAdvances.createdAt));

      return { byStatus, monthly, totals };
    }

    if (type === "transactions") {
      const byType = await db.select({
        type: transactions.type,
        count: sql<number>`COUNT(*)::int`,
        volume: sql<string>`COALESCE(SUM(${transactions.amount}::numeric), 0)::text`,
      }).from(transactions)
        .where(dateFilter(transactions.createdAt))
        .groupBy(transactions.type);

      const daily = await db.select({
        date: sql<string>`DATE(${transactions.createdAt})::text`,
        count: sql<number>`COUNT(*)::int`,
        inVolume: sql<string>`COALESCE(SUM(CASE WHEN type = 'in' THEN ${transactions.amount}::numeric ELSE 0 END), 0)::text`,
        outVolume: sql<string>`COALESCE(SUM(CASE WHEN type = 'out' THEN ${transactions.amount}::numeric ELSE 0 END), 0)::text`,
      }).from(transactions)
        .where(dateFilter(transactions.createdAt))
        .groupBy(sql`DATE(${transactions.createdAt})`)
        .orderBy(sql`DATE(${transactions.createdAt})`);

      const byStatus = await db.select({
        status: transactions.status,
        count: sql<number>`COUNT(*)::int`,
        volume: sql<string>`COALESCE(SUM(${transactions.amount}::numeric), 0)::text`,
      }).from(transactions)
        .where(dateFilter(transactions.createdAt))
        .groupBy(transactions.status);

      const [totals] = await db.select({
        totalCount: sql<number>`COUNT(*)::int`,
        totalIn: sql<string>`COALESCE(SUM(CASE WHEN type = 'in' THEN ${transactions.amount}::numeric ELSE 0 END), 0)::text`,
        totalOut: sql<string>`COALESCE(SUM(CASE WHEN type = 'out' THEN ${transactions.amount}::numeric ELSE 0 END), 0)::text`,
      }).from(transactions).where(dateFilter(transactions.createdAt));

      return { byType, daily, byStatus, totals };
    }

    return {};
  }

  async getDashboardStats(userId: string, options?: { rangeDays?: number; workspaceBusinessId?: string | null }) {
    {
      const ws = options?.workspaceBusinessId?.trim();
      const workspaceOpts =
        ws && ws.length > 0 ? { workspaceBusinessId: ws } : undefined;
      const wallet = await this.getUserWalletBalance(userId, workspaceOpts);
      const currentUser = await this.getUserById(userId);
      const ownBusiness = await this.getBusinessByUserId(userId);
      const businessId =
        ws && ws.length > 0 ? ws : currentUser?.businessId || ownBusiness?.id;

      let actorIds = [userId];
      if (businessId) {
        const members = await this.getUsersByBusinessId(businessId);
        const ownerUserId = (await this.getBusinessById(businessId))?.userId ?? null;
        actorIds = Array.from(new Set([
          userId,
          ...members.map((m) => m.id),
          ...(ownerUserId ? [ownerUserId] : []),
        ]));
      }
      const actorIdsSql = sql.join(actorIds.map((id) => sql`${id}`), sql`, `);

      const completedPaymentsConditions: SQL[] = [
        sql`${bulkPaymentBatches.createdByUserId} IN (${actorIdsSql})`,
        eq(bulkPaymentItems.status, "completed"),
      ];
      if (options?.rangeDays != null) {
        completedPaymentsConditions.push(
          gte(bulkPaymentBatches.createdAt, new Date(Date.now() - options.rangeDays * 24 * 60 * 60 * 1000)),
        );
      }
      const completedPaymentsWhere = and(...completedPaymentsConditions);

      const [completedPayments] = await db
        .select({
          total: sql<string>`COALESCE(SUM(${bulkPaymentItems.amount}::numeric), 0)::text`,
          count: sql<number>`COUNT(*)::int`,
        })
        .from(bulkPaymentItems)
        .innerJoin(bulkPaymentBatches, eq(bulkPaymentItems.batchId, bulkPaymentBatches.id))
        .where(completedPaymentsWhere);

      const [pendingLinksResult] = await db
        .select({
          count: sql<number>`COUNT(*)::int`,
        })
        .from(paymentLinks)
        .where(
          and(
            sql`${paymentLinks.createdByUserId} IN (${actorIdsSql})`,
            sql`${paymentLinks.status} IN ('active', 'pending_payment')`,
          ),
        );

      const [activeCashAdvanceResult] = await db
        .select({
          total: sql<string>`COALESCE(SUM(${cashAdvances.amount}::numeric), 0)::text`,
        })
        .from(cashAdvances)
        .where(
          and(
            eq(cashAdvances.businessId, businessId || ""),
            eq(cashAdvances.status, "active"),
          ),
        );

      const [pendingApprovalsResult] = await db
        .select({
          count: sql<number>`COUNT(*)::int`,
        })
        .from(approvals)
        .where(
          and(
            sql`${approvals.status} IN ('pending_checker', 'pending_approver')`,
            sql`${approvals.makerId} IN (${actorIdsSql})`,
          ),
        );

      return {
        accountBalance: wallet.availableBalance,
        totalPayments: completedPayments?.total || "0",
        activeCashAdvance: activeCashAdvanceResult?.total || "0",
        paymentCount: Number(completedPayments?.count || 0),
        pendingLinks: Number(pendingLinksResult?.count || 0),
        pendingApprovals: Number(pendingApprovalsResult?.count || 0),
      };
    }
  }

  async nextCentypackFarmerCode(businessId: string): Promise<string> {
    const [row] = await db
      .select({ farmerCode: centypackFarmers.farmerCode })
      .from(centypackFarmers)
      .where(eq(centypackFarmers.businessId, businessId))
      .orderBy(sql`${centypackFarmers.farmerCode} desc`)
      .limit(1);
    if (!row) return "FRM-0001";
    const last = parseInt(row.farmerCode.replace("FRM-", ""), 10) || 0;
    return `FRM-${String(last + 1).padStart(4, "0")}`;
  }

  async listCentypackFarmers(businessId: string, opts: { search?: string; status?: string; limit?: number; offset?: number } = {}): Promise<{ rows: CentypackFarmer[]; total: number }> {
    const { search, status, limit = 50, offset = 0 } = opts;
    const conditions: SQL[] = [eq(centypackFarmers.businessId, businessId)];
    if (status) conditions.push(eq(centypackFarmers.status, status));
    if (search) {
      const pattern = `%${search}%`;
      conditions.push(or(
        ilike(centypackFarmers.name, pattern),
        ilike(centypackFarmers.farmerCode, pattern),
        ilike(centypackFarmers.phone, pattern),
        ilike(centypackFarmers.organization, pattern),
      ) as SQL);
    }
    const where = and(...conditions);
    const [{ total }] = await db.select({ total: sql<number>`count(*)::int` }).from(centypackFarmers).where(where);
    const rows = await db.select().from(centypackFarmers).where(where)
      .orderBy(asc(centypackFarmers.farmerCode))
      .limit(limit).offset(offset);
    return { rows, total: total ?? 0 };
  }

  async getCentypackFarmerById(id: string, businessId: string): Promise<CentypackFarmer | null> {
    const [row] = await db.select().from(centypackFarmers)
      .where(and(eq(centypackFarmers.id, id), eq(centypackFarmers.businessId, businessId)))
      .limit(1);
    return row ?? null;
  }

  async createCentypackFarmer(data: Omit<InsertCentypackFarmer, "id" | "createdAt" | "updatedAt">): Promise<CentypackFarmer> {
    const [row] = await db.insert(centypackFarmers)
      .values({ id: crypto.randomUUID(), ...data })
      .returning();
    return row;
  }

  async updateCentypackFarmer(id: string, businessId: string, data: Partial<Omit<InsertCentypackFarmer, "id" | "businessId" | "farmerCode" | "createdAt">>): Promise<CentypackFarmer | null> {
    const [row] = await db.update(centypackFarmers)
      .set({ ...data, updatedAt: new Date() })
      .where(and(eq(centypackFarmers.id, id), eq(centypackFarmers.businessId, businessId)))
      .returning();
    return row ?? null;
  }

  async nextCentypackCropCode(businessId: string): Promise<string> {
    const [row] = await db.select({ cropCode: centypackCrops.cropCode })
      .from(centypackCrops)
      .where(eq(centypackCrops.businessId, businessId))
      .orderBy(sql`${centypackCrops.cropCode} desc`)
      .limit(1);
    if (!row) return "CRP-0001";
    const last = parseInt(row.cropCode.replace("CRP-", ""), 10) || 0;
    return `CRP-${String(last + 1).padStart(4, "0")}`;
  }

  async listCentypackCrops(businessId: string, opts: { search?: string; status?: string; limit?: number; offset?: number } = {}): Promise<{ rows: CentypackCrop[]; total: number }> {
    const { search, status, limit = 50, offset = 0 } = opts;
    const conditions: SQL[] = [eq(centypackCrops.businessId, businessId)];
    if (status) conditions.push(eq(centypackCrops.status, status));
    if (search) {
      const pattern = `%${search}%`;
      conditions.push(or(
        ilike(centypackCrops.name, pattern),
        ilike(centypackCrops.cropCode, pattern),
        ilike(centypackCrops.category, pattern),
      ) as SQL);
    }
    const where = and(...conditions);
    const [{ total }] = await db.select({ total: sql<number>`count(*)::int` }).from(centypackCrops).where(where);
    const rows = await db.select().from(centypackCrops).where(where)
      .orderBy(asc(centypackCrops.cropCode))
      .limit(limit).offset(offset);
    return { rows, total: total ?? 0 };
  }

  async getCentypackCropById(id: string, businessId: string): Promise<CentypackCrop | null> {
    const [row] = await db.select().from(centypackCrops)
      .where(and(eq(centypackCrops.id, id), eq(centypackCrops.businessId, businessId)))
      .limit(1);
    return row ?? null;
  }

  async createCentypackCrop(data: Omit<InsertCentypackCrop, "id" | "createdAt" | "updatedAt">): Promise<CentypackCrop> {
    const [row] = await db.insert(centypackCrops)
      .values({ id: crypto.randomUUID(), ...data })
      .returning();
    return row;
  }

  async updateCentypackCrop(id: string, businessId: string, data: Partial<Omit<InsertCentypackCrop, "id" | "businessId" | "cropCode" | "createdAt" | "updatedAt">>): Promise<CentypackCrop | null> {
    const [row] = await db.update(centypackCrops)
      .set({ ...data, updatedAt: new Date() })
      .where(and(eq(centypackCrops.id, id), eq(centypackCrops.businessId, businessId)))
      .returning();
    return row ?? null;
  }

  async nextCentypackVarietyCode(businessId: string): Promise<string> {
    const [row] = await db.select({ varietyCode: centypackVarieties.varietyCode })
      .from(centypackVarieties)
      .where(eq(centypackVarieties.businessId, businessId))
      .orderBy(sql`${centypackVarieties.varietyCode} desc`)
      .limit(1);
    if (!row) return "VAR-0001";
    const last = parseInt(row.varietyCode.replace("VAR-", ""), 10) || 0;
    return `VAR-${String(last + 1).padStart(4, "0")}`;
  }

  async listCentypackVarieties(businessId: string, opts: { search?: string; cropId?: string; status?: string; limit?: number; offset?: number } = {}): Promise<{ rows: CentypackVariety[]; total: number }> {
    const { search, cropId, status, limit = 50, offset = 0 } = opts;
    const conditions: SQL[] = [eq(centypackVarieties.businessId, businessId)];
    if (cropId) conditions.push(eq(centypackVarieties.cropId, cropId));
    if (status) conditions.push(eq(centypackVarieties.status, status));
    if (search) {
      const pattern = `%${search}%`;
      conditions.push(or(
        ilike(centypackVarieties.name, pattern),
        ilike(centypackVarieties.varietyCode, pattern),
      ) as SQL);
    }
    const where = and(...conditions);
    const [{ total }] = await db.select({ total: sql<number>`count(*)::int` }).from(centypackVarieties).where(where);
    const rows = await db.select().from(centypackVarieties).where(where)
      .orderBy(asc(centypackVarieties.varietyCode))
      .limit(limit).offset(offset);
    return { rows, total: total ?? 0 };
  }

  async getCentypackVarietyById(id: string, businessId: string): Promise<CentypackVariety | null> {
    const [row] = await db.select().from(centypackVarieties)
      .where(and(eq(centypackVarieties.id, id), eq(centypackVarieties.businessId, businessId)))
      .limit(1);
    return row ?? null;
  }

  async createCentypackVariety(data: Omit<InsertCentypackVariety, "id" | "createdAt" | "updatedAt">): Promise<CentypackVariety> {
    const [row] = await db.insert(centypackVarieties)
      .values({ id: crypto.randomUUID(), ...data })
      .returning();
    return row;
  }

  async updateCentypackVariety(id: string, businessId: string, data: Partial<Omit<InsertCentypackVariety, "id" | "businessId" | "varietyCode" | "createdAt" | "updatedAt">>): Promise<CentypackVariety | null> {
    const [row] = await db.update(centypackVarieties)
      .set({ ...data, updatedAt: new Date() })
      .where(and(eq(centypackVarieties.id, id), eq(centypackVarieties.businessId, businessId)))
      .returning();
    return row ?? null;
  }

  async nextCentypackCartonTypeCode(businessId: string): Promise<string> {
    const [row] = await db.select({ cartonCode: centypackCartonTypes.cartonCode })
      .from(centypackCartonTypes)
      .where(eq(centypackCartonTypes.businessId, businessId))
      .orderBy(sql`${centypackCartonTypes.cartonCode} desc`)
      .limit(1);
    if (!row) return "CTN-0001";
    const last = parseInt(row.cartonCode.replace("CTN-", ""), 10) || 0;
    return `CTN-${String(last + 1).padStart(4, "0")}`;
  }

  async listCentypackCartonTypes(businessId: string, opts: { search?: string; status?: string; limit?: number; offset?: number } = {}): Promise<{ rows: CentypackCartonType[]; total: number }> {
    const { search, status, limit = 50, offset = 0 } = opts;
    const conditions: SQL[] = [eq(centypackCartonTypes.businessId, businessId)];
    if (status) conditions.push(eq(centypackCartonTypes.status, status));
    if (search) {
      const pattern = `%${search}%`;
      conditions.push(or(
        ilike(centypackCartonTypes.name, pattern),
        ilike(centypackCartonTypes.cartonCode, pattern),
        ilike(centypackCartonTypes.dimensions, pattern),
      ) as SQL);
    }
    const where = and(...conditions);
    const [{ total }] = await db.select({ total: sql<number>`count(*)::int` }).from(centypackCartonTypes).where(where);
    const rows = await db.select().from(centypackCartonTypes).where(where)
      .orderBy(asc(centypackCartonTypes.cartonCode))
      .limit(limit).offset(offset);
    return { rows, total: total ?? 0 };
  }

  async getCentypackCartonTypeById(id: string, businessId: string): Promise<CentypackCartonType | null> {
    const [row] = await db.select().from(centypackCartonTypes)
      .where(and(eq(centypackCartonTypes.id, id), eq(centypackCartonTypes.businessId, businessId)))
      .limit(1);
    return row ?? null;
  }

  async createCentypackCartonType(data: Omit<InsertCentypackCartonType, "id" | "createdAt" | "updatedAt">): Promise<CentypackCartonType> {
    const [row] = await db.insert(centypackCartonTypes)
      .values({ id: crypto.randomUUID(), ...data })
      .returning();
    return row;
  }

  async updateCentypackCartonType(id: string, businessId: string, data: Partial<Omit<InsertCentypackCartonType, "id" | "businessId" | "cartonCode" | "createdAt" | "updatedAt">>): Promise<CentypackCartonType | null> {
    const [row] = await db.update(centypackCartonTypes)
      .set({ ...data, updatedAt: new Date() })
      .where(and(eq(centypackCartonTypes.id, id), eq(centypackCartonTypes.businessId, businessId)))
      .returning();
    return row ?? null;
  }

  async nextCentypackWorkerCategoryCode(businessId: string): Promise<string> {
    const [row] = await db.select({ workerCategoryCode: centypackWorkerCategories.workerCategoryCode })
      .from(centypackWorkerCategories)
      .where(eq(centypackWorkerCategories.businessId, businessId))
      .orderBy(sql`${centypackWorkerCategories.workerCategoryCode} desc`)
      .limit(1);
    if (!row) return "WCT-0001";
    const last = parseInt(row.workerCategoryCode.replace("WCT-", ""), 10) || 0;
    return `WCT-${String(last + 1).padStart(4, "0")}`;
  }

  async listCentypackWorkerCategories(businessId: string, opts: { search?: string; status?: string; limit?: number; offset?: number } = {}): Promise<{ rows: CentypackWorkerCategory[]; total: number }> {
    const { search, status, limit = 50, offset = 0 } = opts;
    const conditions: SQL[] = [eq(centypackWorkerCategories.businessId, businessId)];
    if (status) conditions.push(eq(centypackWorkerCategories.status, status));
    if (search) {
      const pattern = `%${search}%`;
      conditions.push(or(
        ilike(centypackWorkerCategories.name, pattern),
        ilike(centypackWorkerCategories.workerCategoryCode, pattern),
      ) as SQL);
    }
    const where = and(...conditions);
    const [{ total }] = await db.select({ total: sql<number>`count(*)::int` }).from(centypackWorkerCategories).where(where);
    const rows = await db.select().from(centypackWorkerCategories).where(where)
      .orderBy(asc(centypackWorkerCategories.workerCategoryCode))
      .limit(limit).offset(offset);
    return { rows, total: total ?? 0 };
  }

  async getCentypackWorkerCategoryById(id: string, businessId: string): Promise<CentypackWorkerCategory | null> {
    const [row] = await db.select().from(centypackWorkerCategories)
      .where(and(eq(centypackWorkerCategories.id, id), eq(centypackWorkerCategories.businessId, businessId)))
      .limit(1);
    return row ?? null;
  }

  async createCentypackWorkerCategory(data: Omit<InsertCentypackWorkerCategory, "id" | "createdAt" | "updatedAt">): Promise<CentypackWorkerCategory> {
    const [row] = await db.insert(centypackWorkerCategories)
      .values({ id: crypto.randomUUID(), ...data })
      .returning();
    return row;
  }

  async updateCentypackWorkerCategory(id: string, businessId: string, data: Partial<Omit<InsertCentypackWorkerCategory, "id" | "businessId" | "workerCategoryCode" | "createdAt" | "updatedAt">>): Promise<CentypackWorkerCategory | null> {
    const [row] = await db.update(centypackWorkerCategories)
      .set({ ...data, updatedAt: new Date() })
      .where(and(eq(centypackWorkerCategories.id, id), eq(centypackWorkerCategories.businessId, businessId)))
      .returning();
    return row ?? null;
  }

  async nextCentypackWarehouseCode(businessId: string): Promise<string> {
    const [row] = await db.select({ warehouseCode: centypackWarehouses.warehouseCode })
      .from(centypackWarehouses).where(eq(centypackWarehouses.businessId, businessId))
      .orderBy(sql`${centypackWarehouses.warehouseCode} desc`).limit(1);
    if (!row) return "WHZ-0001";
    const last = parseInt(row.warehouseCode.replace("WHZ-", ""), 10) || 0;
    return `WHZ-${String(last + 1).padStart(4, "0")}`;
  }

  async listCentypackWarehouses(businessId: string, opts: { search?: string; status?: string; limit?: number; offset?: number } = {}): Promise<{ rows: CentypackWarehouse[]; total: number }> {
    const { search, status, limit = 50, offset = 0 } = opts;
    const conditions: SQL[] = [eq(centypackWarehouses.businessId, businessId)];
    if (status) conditions.push(eq(centypackWarehouses.status, status));
    if (search) {
      const pattern = `%${search}%`;
      conditions.push(or(ilike(centypackWarehouses.name, pattern), ilike(centypackWarehouses.warehouseCode, pattern), ilike(centypackWarehouses.purpose, pattern)) as SQL);
    }
    const where = and(...conditions);
    const [{ total }] = await db.select({ total: sql<number>`count(*)::int` }).from(centypackWarehouses).where(where);
    const rows = await db.select().from(centypackWarehouses).where(where).orderBy(asc(centypackWarehouses.warehouseCode)).limit(limit).offset(offset);
    return { rows, total: total ?? 0 };
  }

  async getCentypackWarehouseById(id: string, businessId: string): Promise<CentypackWarehouse | null> {
    const [row] = await db.select().from(centypackWarehouses).where(and(eq(centypackWarehouses.id, id), eq(centypackWarehouses.businessId, businessId))).limit(1);
    return row ?? null;
  }

  async createCentypackWarehouse(data: Omit<InsertCentypackWarehouse, "id" | "createdAt" | "updatedAt">): Promise<CentypackWarehouse> {
    const [row] = await db.insert(centypackWarehouses).values({ id: crypto.randomUUID(), ...data }).returning();
    return row;
  }

  async updateCentypackWarehouse(id: string, businessId: string, data: Partial<Omit<InsertCentypackWarehouse, "id" | "businessId" | "warehouseCode" | "createdAt" | "updatedAt">>): Promise<CentypackWarehouse | null> {
    const [row] = await db.update(centypackWarehouses).set({ ...data, updatedAt: new Date() }).where(and(eq(centypackWarehouses.id, id), eq(centypackWarehouses.businessId, businessId))).returning();
    return row ?? null;
  }

  async nextCentypackGradeCode(businessId: string): Promise<string> {
    const [row] = await db.select({ gradeCode: centypackGradeCodes.gradeCode })
      .from(centypackGradeCodes).where(eq(centypackGradeCodes.businessId, businessId))
      .orderBy(sql`${centypackGradeCodes.gradeCode} desc`).limit(1);
    if (!row) return "GRD-0001";
    const last = parseInt(row.gradeCode.replace("GRD-", ""), 10) || 0;
    return `GRD-${String(last + 1).padStart(4, "0")}`;
  }

  async listCentypackGradeCodes(businessId: string, opts: { search?: string; status?: string; limit?: number; offset?: number } = {}): Promise<{ rows: CentypackGradeCode[]; total: number }> {
    const { search, status, limit = 50, offset = 0 } = opts;
    const conditions: SQL[] = [eq(centypackGradeCodes.businessId, businessId)];
    if (status) conditions.push(eq(centypackGradeCodes.status, status));
    if (search) {
      const pattern = `%${search}%`;
      conditions.push(or(ilike(centypackGradeCodes.name, pattern), ilike(centypackGradeCodes.gradeCode, pattern)) as SQL);
    }
    const where = and(...conditions);
    const [{ total }] = await db.select({ total: sql<number>`count(*)::int` }).from(centypackGradeCodes).where(where);
    const rows = await db.select().from(centypackGradeCodes).where(where).orderBy(asc(centypackGradeCodes.gradeCode)).limit(limit).offset(offset);
    return { rows, total: total ?? 0 };
  }

  async getCentypackGradeCodeById(id: string, businessId: string): Promise<CentypackGradeCode | null> {
    const [row] = await db.select().from(centypackGradeCodes).where(and(eq(centypackGradeCodes.id, id), eq(centypackGradeCodes.businessId, businessId))).limit(1);
    return row ?? null;
  }

  async createCentypackGradeCode(data: Omit<InsertCentypackGradeCode, "id" | "createdAt" | "updatedAt">): Promise<CentypackGradeCode> {
    const [row] = await db.insert(centypackGradeCodes).values({ id: crypto.randomUUID(), ...data }).returning();
    return row;
  }

  async updateCentypackGradeCode(id: string, businessId: string, data: Partial<Omit<InsertCentypackGradeCode, "id" | "businessId" | "gradeCode" | "createdAt" | "updatedAt">>): Promise<CentypackGradeCode | null> {
    const [row] = await db.update(centypackGradeCodes).set({ ...data, updatedAt: new Date() }).where(and(eq(centypackGradeCodes.id, id), eq(centypackGradeCodes.businessId, businessId))).returning();
    return row ?? null;
  }

  async nextCentypackDefectTypeCode(businessId: string): Promise<string> {
    const [row] = await db.select({ defectCode: centypackDefectTypes.defectCode })
      .from(centypackDefectTypes).where(eq(centypackDefectTypes.businessId, businessId))
      .orderBy(sql`${centypackDefectTypes.defectCode} desc`).limit(1);
    if (!row) return "DFT-0001";
    const last = parseInt(row.defectCode.replace("DFT-", ""), 10) || 0;
    return `DFT-${String(last + 1).padStart(4, "0")}`;
  }

  async listCentypackDefectTypes(businessId: string, opts: { search?: string; status?: string; limit?: number; offset?: number } = {}): Promise<{ rows: CentypackDefectType[]; total: number }> {
    const { search, status, limit = 50, offset = 0 } = opts;
    const conditions: SQL[] = [eq(centypackDefectTypes.businessId, businessId)];
    if (status) conditions.push(eq(centypackDefectTypes.status, status));
    if (search) {
      const pattern = `%${search}%`;
      conditions.push(or(ilike(centypackDefectTypes.name, pattern), ilike(centypackDefectTypes.defectCode, pattern)) as SQL);
    }
    const where = and(...conditions);
    const [{ total }] = await db.select({ total: sql<number>`count(*)::int` }).from(centypackDefectTypes).where(where);
    const rows = await db.select().from(centypackDefectTypes).where(where).orderBy(asc(centypackDefectTypes.defectCode)).limit(limit).offset(offset);
    return { rows, total: total ?? 0 };
  }

  async getCentypackDefectTypeById(id: string, businessId: string): Promise<CentypackDefectType | null> {
    const [row] = await db.select().from(centypackDefectTypes).where(and(eq(centypackDefectTypes.id, id), eq(centypackDefectTypes.businessId, businessId))).limit(1);
    return row ?? null;
  }

  async createCentypackDefectType(data: Omit<InsertCentypackDefectType, "id" | "createdAt" | "updatedAt">): Promise<CentypackDefectType> {
    const [row] = await db.insert(centypackDefectTypes).values({ id: crypto.randomUUID(), ...data }).returning();
    return row;
  }

  async updateCentypackDefectType(id: string, businessId: string, data: Partial<Omit<InsertCentypackDefectType, "id" | "businessId" | "defectCode" | "createdAt" | "updatedAt">>): Promise<CentypackDefectType | null> {
    const [row] = await db.update(centypackDefectTypes).set({ ...data, updatedAt: new Date() }).where(and(eq(centypackDefectTypes.id, id), eq(centypackDefectTypes.businessId, businessId))).returning();
    return row ?? null;
  }

  async nextCentypackIntakeCode(businessId: string): Promise<string> {
    const [row] = await db.select({ intakeCode: centypackIntake.intakeCode })
      .from(centypackIntake).where(eq(centypackIntake.businessId, businessId))
      .orderBy(sql`${centypackIntake.intakeCode} desc`).limit(1);
    if (!row) return "INT-0001";
    const last = parseInt(row.intakeCode.replace("INT-", ""), 10) || 0;
    return `INT-${String(last + 1).padStart(4, "0")}`;
  }

  async nextCentypackBatchCode(businessId: string): Promise<string> {
    const [row] = await db.select({ batchCode: centypackIntake.batchCode })
      .from(centypackIntake).where(and(eq(centypackIntake.businessId, businessId), isNotNull(centypackIntake.batchCode)))
      .orderBy(sql`${centypackIntake.batchCode} desc`).limit(1);
    if (!row?.batchCode) return "BT-0001";
    const last = parseInt(row.batchCode.replace("BT-", ""), 10) || 0;
    return `BT-${String(last + 1).padStart(4, "0")}`;
  }

  async listCentypackIntake(businessId: string, opts: { search?: string; farmerId?: string; cropId?: string; status?: string; dateFrom?: string; dateTo?: string; limit?: number; offset?: number; excludeGraded?: boolean; includeIntakeId?: string } = {}): Promise<{ rows: CentypackIntake[]; total: number }> {
    const { search, farmerId, cropId, status, dateFrom, dateTo, limit = 50, offset = 0, excludeGraded, includeIntakeId } = opts;
    const conditions: SQL[] = [eq(centypackIntake.businessId, businessId)];
    if (farmerId) conditions.push(eq(centypackIntake.farmerId, farmerId));
    if (cropId) conditions.push(eq(centypackIntake.cropId, cropId));
    if (status) conditions.push(eq(centypackIntake.status, status));
    if (dateFrom) conditions.push(gte(centypackIntake.intakeDate, dateFrom));
    if (dateTo) conditions.push(lte(centypackIntake.intakeDate, dateTo));
    if (search) {
      const pattern = `%${search}%`;
      conditions.push(or(
        ilike(centypackIntake.intakeCode, pattern),
        ilike(centypackIntake.vehicleReg, pattern),
        ilike(centypackIntake.notes, pattern),
      ) as SQL);
    }
    if (excludeGraded) {
      const notGraded = sql`NOT EXISTS (
        SELECT 1 FROM centypack_grading_sessions gs
        WHERE gs.intake_id = ${centypackIntake.id}
        AND gs.business_id = ${businessId}
        AND gs.status = 'completed'
      )` as SQL;
      conditions.push(
        includeIntakeId
          ? sql`(${notGraded} OR ${centypackIntake.id} = ${includeIntakeId})` as SQL
          : notGraded,
      );
    }
    const where = and(...conditions);
    const [{ total }] = await db.select({ total: sql<number>`count(*)::int` }).from(centypackIntake).where(where);
    const rows = await db.select().from(centypackIntake).where(where)
      .orderBy(sql`${centypackIntake.intakeDate} desc, ${centypackIntake.intakeCode} desc`)
      .limit(limit).offset(offset);
    return { rows, total: total ?? 0 };
  }

  async getCentypackIntakeById(id: string, businessId: string): Promise<CentypackIntake | null> {
    const [row] = await db.select().from(centypackIntake).where(and(eq(centypackIntake.id, id), eq(centypackIntake.businessId, businessId))).limit(1);
    return row ?? null;
  }

  async createCentypackIntake(data: Omit<InsertCentypackIntake, "id" | "createdAt" | "updatedAt">): Promise<CentypackIntake> {
    const id = crypto.randomUUID();
    const [row] = await db.insert(centypackIntake).values({ id, ...data }).returning();
    const client = await pool.connect();
    try {
      await client.query("BEGIN");
      const entries = data.status === "cancelled" ? [] : [{
        txnType: "intake_in", txnDate: data.intakeDate, cropId: data.cropId,
        varietyId: data.varietyId ?? null, stage: "raw", direction: "in",
        qtyKg: String(data.netWeightKg),
      }];
      await this.syncLedgerInTxn(client, id, data.businessId, entries);
      await client.query("COMMIT");
    } catch { await client.query("ROLLBACK"); } finally { client.release(); }
    return row;
  }

  async updateCentypackIntake(id: string, businessId: string, data: Partial<Omit<InsertCentypackIntake, "id" | "businessId" | "intakeCode" | "createdAt" | "updatedAt">>): Promise<CentypackIntake | null> {
    const [row] = await db.update(centypackIntake).set({ ...data, updatedAt: new Date() }).where(and(eq(centypackIntake.id, id), eq(centypackIntake.businessId, businessId))).returning();
    if (!row) return null;
    const client = await pool.connect();
    try {
      await client.query("BEGIN");
      const entries = row.status === "cancelled" ? [] : [{
        txnType: "intake_in", txnDate: row.intakeDate, cropId: row.cropId,
        varietyId: row.varietyId ?? null, stage: "raw", direction: "in",
        qtyKg: String(row.netWeightKg),
      }];
      await this.syncLedgerInTxn(client, id, businessId, entries);
      await client.query("COMMIT");
    } catch { await client.query("ROLLBACK"); } finally { client.release(); }
    return row;
  }

  // CentyPack GDNs
  async nextCentypackGdnCode(businessId: string): Promise<string> {
    const [row] = await db.select({ gdnCode: centypackGdns.gdnCode }).from(centypackGdns)
      .where(eq(centypackGdns.businessId, businessId))
      .orderBy(sql`${centypackGdns.gdnCode} desc`).limit(1);
    if (!row) return "GDN-0001";
    const last = parseInt(row.gdnCode.replace("GDN-", ""), 10) || 0;
    return `GDN-${String(last + 1).padStart(4, "0")}`;
  }

  async listCentypackGdns(businessId: string, opts: { search?: string; type?: string; status?: string; dateFrom?: string; dateTo?: string; limit?: number; offset?: number } = {}): Promise<{ rows: (CentypackGdn & { totalWeightKg: number; itemCount: number; batchCodes: string[] })[]; total: number }> {
    const { search, type, status, dateFrom, dateTo, limit = 50, offset = 0 } = opts;
    const conditions: SQL[] = [eq(centypackGdns.businessId, businessId)];
    if (type) conditions.push(eq(centypackGdns.type, type));
    if (status) conditions.push(eq(centypackGdns.status, status));
    if (dateFrom) conditions.push(gte(centypackGdns.gdnDate, dateFrom));
    if (dateTo) conditions.push(lte(centypackGdns.gdnDate, dateTo));
    if (search) {
      const pattern = `%${search}%`;
      conditions.push(or(
        ilike(centypackGdns.gdnCode, pattern),
        ilike(centypackGdns.customerName, pattern),
        ilike(centypackGdns.vehicleReg, pattern),
        ilike(centypackGdns.notes, pattern),
      ) as SQL);
    }
    const where = and(...conditions);
    const [{ total }] = await db.select({ total: sql<number>`count(*)::int` }).from(centypackGdns).where(where);
    const rows = await db
      .select({
        id: centypackGdns.id,
        businessId: centypackGdns.businessId,
        gdnCode: centypackGdns.gdnCode,
        gdnDate: centypackGdns.gdnDate,
        type: centypackGdns.type,
        customerName: centypackGdns.customerName,
        fromWarehouseId: centypackGdns.fromWarehouseId,
        toWarehouseId: centypackGdns.toWarehouseId,
        vehicleReg: centypackGdns.vehicleReg,
        notes: centypackGdns.notes,
        status: centypackGdns.status,
        createdAt: centypackGdns.createdAt,
        updatedAt: centypackGdns.updatedAt,
        totalWeightKg: sql<number>`coalesce(sum(${centypackGdnItems.netWeightKg}::numeric), 0)::float`,
        itemCount: sql<number>`count(distinct ${centypackGdnItems.id})::int`,
        batchCodes: sql<string[]>`coalesce(array_remove(array_agg(distinct ${centypackIntake.batchCode}), null), '{}')`,
      })
      .from(centypackGdns)
      .leftJoin(centypackGdnItems, eq(centypackGdnItems.gdnId, centypackGdns.id))
      .leftJoin(centypackIntake, eq(centypackIntake.id, centypackGdnItems.intakeId))
      .where(where)
      .groupBy(centypackGdns.id)
      .orderBy(sql`${centypackGdns.gdnDate} desc, ${centypackGdns.gdnCode} desc`)
      .limit(limit).offset(offset);
    return { rows: rows as (CentypackGdn & { totalWeightKg: number; itemCount: number; batchCodes: string[] })[], total: total ?? 0 };
  }

  async getCentypackGdnById(id: string, businessId: string): Promise<(CentypackGdn & { items: CentypackGdnItem[] }) | null> {
    const [header] = await db.select().from(centypackGdns).where(and(eq(centypackGdns.id, id), eq(centypackGdns.businessId, businessId))).limit(1);
    if (!header) return null;
    const items = await db.select().from(centypackGdnItems).where(eq(centypackGdnItems.gdnId, id));
    return { ...header, items };
  }

  async createCentypackGdn(
    header: Omit<InsertCentypackGdn, "id" | "createdAt" | "updatedAt">,
    items: Omit<InsertCentypackGdnItem, "id" | "gdnId" | "businessId" | "createdAt">[],
  ): Promise<CentypackGdn & { items: CentypackGdnItem[] }> {
    const gdnId = crypto.randomUUID();
    const client: PoolClient = await pool.connect();
    try {
      await client.query("BEGIN");
      const { rows: [gdn] } = await client.query<CentypackGdn>(
        `INSERT INTO centypack_gdns (id, business_id, gdn_code, gdn_date, type, customer_name, from_warehouse_id, to_warehouse_id, vehicle_reg, notes, status)
         VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11) RETURNING *`,
        [gdnId, header.businessId, header.gdnCode, header.gdnDate, header.type, header.customerName ?? null, header.fromWarehouseId ?? null, header.toWarehouseId ?? null, header.vehicleReg ?? null, header.notes ?? null, header.status ?? "draft"],
      );
      const insertedItems: CentypackGdnItem[] = [];
      for (const item of items) {
        const itemId = crypto.randomUUID();
        const { rows: [ins] } = await client.query<CentypackGdnItem>(
          `INSERT INTO centypack_gdn_items (id, gdn_id, business_id, crop_id, variety_id, carton_type_id, carton_count, net_weight_kg, grade_code_id, intake_id, notes)
           VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11) RETURNING *`,
          [itemId, gdnId, header.businessId, item.cropId, item.varietyId ?? null, item.cartonTypeId ?? null, item.cartonCount ?? null, item.netWeightKg, item.gradeCodeId ?? null, (item as any).intakeId ?? null, item.notes ?? null],
        );
        insertedItems.push(ins);
      }
      if (gdn.status === "confirmed") {
        const gdnCreateEntries = insertedItems.flatMap(item =>
          gdn.type === "transfer"
            ? [
                { txnType: "transfer_out", txnDate: gdn.gdn_date, cropId: item.crop_id, varietyId: item.variety_id ?? null, gradeCodeId: item.grade_code_id ?? null, cartonTypeId: item.carton_type_id ?? null, warehouseId: gdn.from_warehouse_id ?? null, stage: "packed", direction: "out", qtyKg: String(item.net_weight_kg), cartonCount: item.carton_count ?? null },
                { txnType: "transfer_in",  txnDate: gdn.gdn_date, cropId: item.crop_id, varietyId: item.variety_id ?? null, gradeCodeId: item.grade_code_id ?? null, cartonTypeId: item.carton_type_id ?? null, warehouseId: gdn.to_warehouse_id ?? null,   stage: "packed", direction: "in",  qtyKg: String(item.net_weight_kg), cartonCount: item.carton_count ?? null },
              ]
            : [{ txnType: "dispatch_out", txnDate: gdn.gdn_date, cropId: item.crop_id, varietyId: item.variety_id ?? null, gradeCodeId: item.grade_code_id ?? null, cartonTypeId: item.carton_type_id ?? null, warehouseId: gdn.from_warehouse_id ?? null, stage: "packed", direction: "out", qtyKg: String(item.net_weight_kg), cartonCount: item.carton_count ?? null }],
        );
        await client.query("SAVEPOINT before_ledger");
        try {
          await this.syncLedgerInTxn(client, gdnId, header.businessId, gdnCreateEntries);
        } catch (ledgerErr) {
          console.error("[centypack] ledger sync failed on gdn create", gdnId, ledgerErr);
          await client.query("ROLLBACK TO SAVEPOINT before_ledger");
        }
      } else {
        await client.query("SAVEPOINT before_ledger");
        try {
          await this.syncLedgerInTxn(client, gdnId, header.businessId, []);
        } catch (ledgerErr) {
          console.error("[centypack] ledger sync failed on gdn create (clear)", gdnId, ledgerErr);
          await client.query("ROLLBACK TO SAVEPOINT before_ledger");
        }
      }
      await client.query("COMMIT");
      return { ...gdn, items: insertedItems };
    } catch (err) {
      await client.query("ROLLBACK");
      throw err;
    } finally {
      client.release();
    }
  }

  async updateCentypackGdn(
    id: string,
    businessId: string,
    header: Partial<Omit<InsertCentypackGdn, "id" | "businessId" | "gdnCode" | "createdAt" | "updatedAt">>,
    items?: Omit<InsertCentypackGdnItem, "id" | "gdnId" | "businessId" | "createdAt">[],
  ): Promise<(CentypackGdn & { items: CentypackGdnItem[] }) | null> {
    const existing = await this.getCentypackGdnById(id, businessId);
    if (!existing) return null;
    const client: PoolClient = await pool.connect();
    try {
      await client.query("BEGIN");
      const { rows: [gdn] } = await client.query<CentypackGdn>(
        `UPDATE centypack_gdns SET gdn_date=COALESCE($3,gdn_date), type=COALESCE($4,type), customer_name=COALESCE($5,customer_name), from_warehouse_id=COALESCE($6,from_warehouse_id), to_warehouse_id=COALESCE($7,to_warehouse_id), vehicle_reg=COALESCE($8,vehicle_reg), notes=COALESCE($9,notes), status=COALESCE($10,status), updated_at=now() WHERE id=$1 AND business_id=$2 RETURNING *`,
        [id, businessId, header.gdnDate ?? null, header.type ?? null, header.customerName ?? null, header.fromWarehouseId ?? null, header.toWarehouseId ?? null, header.vehicleReg ?? null, header.notes ?? null, header.status ?? null],
      );
      let updatedItems = existing.items;
      if (items !== undefined) {
        await client.query(`DELETE FROM centypack_gdn_items WHERE gdn_id=$1`, [id]);
        updatedItems = [];
        for (const item of items) {
          const itemId = crypto.randomUUID();
          const { rows: [ins] } = await client.query<CentypackGdnItem>(
            `INSERT INTO centypack_gdn_items (id, gdn_id, business_id, crop_id, variety_id, carton_type_id, carton_count, net_weight_kg, grade_code_id, notes)
             VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10) RETURNING *`,
            [itemId, id, businessId, item.cropId, item.varietyId ?? null, item.cartonTypeId ?? null, item.cartonCount ?? null, item.netWeightKg, item.gradeCodeId ?? null, item.notes ?? null],
          );
          updatedItems.push(ins);
        }
      }
      if (gdn.status === "confirmed") {
        const gdnUpdateEntries = updatedItems.flatMap(item =>
          gdn.type === "transfer"
            ? [
                { txnType: "transfer_out", txnDate: gdn.gdn_date, cropId: item.crop_id, varietyId: item.variety_id ?? null, gradeCodeId: item.grade_code_id ?? null, cartonTypeId: item.carton_type_id ?? null, warehouseId: gdn.from_warehouse_id ?? null, stage: "packed", direction: "out", qtyKg: String(item.net_weight_kg), cartonCount: item.carton_count ?? null },
                { txnType: "transfer_in",  txnDate: gdn.gdn_date, cropId: item.crop_id, varietyId: item.variety_id ?? null, gradeCodeId: item.grade_code_id ?? null, cartonTypeId: item.carton_type_id ?? null, warehouseId: gdn.to_warehouse_id ?? null,   stage: "packed", direction: "in",  qtyKg: String(item.net_weight_kg), cartonCount: item.carton_count ?? null },
              ]
            : [{ txnType: "dispatch_out", txnDate: gdn.gdn_date, cropId: item.crop_id, varietyId: item.variety_id ?? null, gradeCodeId: item.grade_code_id ?? null, cartonTypeId: item.carton_type_id ?? null, warehouseId: gdn.from_warehouse_id ?? null, stage: "packed", direction: "out", qtyKg: String(item.net_weight_kg), cartonCount: item.carton_count ?? null }],
        );
        await client.query("SAVEPOINT before_ledger");
        try {
          await this.syncLedgerInTxn(client, id, businessId, gdnUpdateEntries);
        } catch (ledgerErr) {
          console.error("[centypack] ledger sync failed on gdn update", id, ledgerErr);
          await client.query("ROLLBACK TO SAVEPOINT before_ledger");
        }
      } else {
        await client.query("SAVEPOINT before_ledger");
        try {
          await this.syncLedgerInTxn(client, id, businessId, []);
        } catch (ledgerErr) {
          console.error("[centypack] ledger sync failed on gdn update (clear)", id, ledgerErr);
          await client.query("ROLLBACK TO SAVEPOINT before_ledger");
        }
      }
      await client.query("COMMIT");
      return { ...gdn, items: updatedItems };
    } catch (err) {
      await client.query("ROLLBACK");
      throw err;
    } finally {
      client.release();
    }
  }

  // CentyPack grading sessions
  async nextCentypackGradingCode(businessId: string): Promise<string> {
    const [row] = await db.select({ sessionCode: centypackGradingSessions.sessionCode })
      .from(centypackGradingSessions).where(eq(centypackGradingSessions.businessId, businessId))
      .orderBy(sql`${centypackGradingSessions.sessionCode} desc`).limit(1);
    if (!row) return "GRD-0001";
    const last = parseInt(row.sessionCode.replace("GRD-", ""), 10) || 0;
    return `GRD-${String(last + 1).padStart(4, "0")}`;
  }

  async listCentypackGradingSessions(businessId: string, opts: { search?: string; cropId?: string; status?: string; dateFrom?: string; dateTo?: string; limit?: number; offset?: number } = {}): Promise<{ rows: (CentypackGradingSession & { totalGradedKg: number; totalDefectKg: number })[]; total: number }> {
    const { search, cropId, status, dateFrom, dateTo, limit = 50, offset = 0 } = opts;
    const conditions: SQL[] = [eq(centypackGradingSessions.businessId, businessId)];
    if (cropId) conditions.push(eq(centypackGradingSessions.cropId, cropId));
    if (status) conditions.push(eq(centypackGradingSessions.status, status));
    if (dateFrom) conditions.push(gte(centypackGradingSessions.sessionDate, dateFrom));
    if (dateTo) conditions.push(lte(centypackGradingSessions.sessionDate, dateTo));
    if (search) {
      const pattern = `%${search}%`;
      conditions.push(or(
        ilike(centypackGradingSessions.sessionCode, pattern),
        ilike(centypackGradingSessions.notes, pattern),
      ) as SQL);
    }
    const where = and(...conditions);
    const [{ total }] = await db.select({ total: sql<number>`count(*)::int` }).from(centypackGradingSessions).where(where);
    const rows = await db
      .select({
        id: centypackGradingSessions.id,
        businessId: centypackGradingSessions.businessId,
        sessionCode: centypackGradingSessions.sessionCode,
        sessionDate: centypackGradingSessions.sessionDate,
        intakeId: centypackGradingSessions.intakeId,
        cropId: centypackGradingSessions.cropId,
        varietyId: centypackGradingSessions.varietyId,
        inputWeightKg: centypackGradingSessions.inputWeightKg,
        notes: centypackGradingSessions.notes,
        status: centypackGradingSessions.status,
        createdAt: centypackGradingSessions.createdAt,
        updatedAt: centypackGradingSessions.updatedAt,
        totalGradedKg: sql<number>`coalesce((SELECT sum(gl.net_weight_kg::numeric) FROM centypack_grading_lines gl WHERE gl.session_id = "centypack_grading_sessions"."id"), 0)::float`,
        totalDefectKg: sql<number>`coalesce((SELECT sum(gd.weight_kg::numeric) FROM centypack_grading_defects gd WHERE gd.session_id = "centypack_grading_sessions"."id"), 0)::float`,
      })
      .from(centypackGradingSessions)
      .where(where)
      .orderBy(sql`${centypackGradingSessions.sessionDate} desc, ${centypackGradingSessions.sessionCode} desc`)
      .limit(limit).offset(offset);
    return { rows: rows as (CentypackGradingSession & { totalGradedKg: number; totalDefectKg: number })[], total: total ?? 0 };
  }

  async getCentypackGradingSessionById(id: string, businessId: string): Promise<(CentypackGradingSession & { lines: CentypackGradingLine[]; defects: CentypackGradingDefect[] }) | null> {
    const [header] = await db.select().from(centypackGradingSessions).where(and(eq(centypackGradingSessions.id, id), eq(centypackGradingSessions.businessId, businessId))).limit(1);
    if (!header) return null;
    const [lines, defects] = await Promise.all([
      db.select().from(centypackGradingLines).where(eq(centypackGradingLines.sessionId, id)),
      db.select().from(centypackGradingDefects).where(eq(centypackGradingDefects.sessionId, id)),
    ]);
    return { ...header, lines, defects };
  }

  async createCentypackGradingSession(
    header: Omit<InsertCentypackGradingSession, "id" | "createdAt" | "updatedAt">,
    lines: Omit<InsertCentypackGradingLine, "id" | "sessionId" | "businessId" | "createdAt">[],
    defects: Omit<InsertCentypackGradingDefect, "id" | "sessionId" | "businessId" | "createdAt">[],
  ): Promise<CentypackGradingSession & { lines: CentypackGradingLine[]; defects: CentypackGradingDefect[] }> {
    const sessionId = crypto.randomUUID();
    const client: PoolClient = await pool.connect();
    try {
      await client.query("BEGIN");
      const { rows: [session] } = await client.query<CentypackGradingSession>(
        `INSERT INTO centypack_grading_sessions (id, business_id, session_code, session_date, intake_id, crop_id, variety_id, input_weight_kg, notes, status)
         VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10) RETURNING *`,
        [sessionId, header.businessId, header.sessionCode, header.sessionDate, header.intakeId ?? null, header.cropId, header.varietyId ?? null, header.inputWeightKg, header.notes ?? null, header.status ?? "draft"],
      );
      const insertedLines: CentypackGradingLine[] = [];
      for (const line of lines) {
        const { rows: [ins] } = await client.query<CentypackGradingLine>(
          `INSERT INTO centypack_grading_lines (id, session_id, business_id, grade_code_id, net_weight_kg, notes) VALUES ($1,$2,$3,$4,$5,$6) RETURNING *`,
          [crypto.randomUUID(), sessionId, header.businessId, line.gradeCodeId, line.netWeightKg, line.notes ?? null],
        );
        insertedLines.push(ins);
      }
      const insertedDefects: CentypackGradingDefect[] = [];
      for (const defect of defects) {
        const { rows: [ins] } = await client.query<CentypackGradingDefect>(
          `INSERT INTO centypack_grading_defects (id, session_id, business_id, defect_type_id, weight_kg, notes) VALUES ($1,$2,$3,$4,$5,$6) RETURNING *`,
          [crypto.randomUUID(), sessionId, header.businessId, defect.defectTypeId, defect.weightKg, defect.notes ?? null],
        );
        insertedDefects.push(ins);
      }
      const gradingEntries = session.status === "completed"
        ? [
            { txnType: "grading_out", txnDate: session.session_date, cropId: session.crop_id, varietyId: session.variety_id ?? null, stage: "raw", direction: "out", qtyKg: String(session.input_weight_kg) },
            ...insertedLines.map(l => ({ txnType: "grading_in", txnDate: session.session_date, cropId: session.crop_id, varietyId: session.variety_id ?? null, gradeCodeId: (l as any).grade_code_id ?? null, stage: "graded", direction: "in", qtyKg: String((l as any).net_weight_kg) })),
            ...insertedDefects.map(d => ({ txnType: "grading_waste", txnDate: session.session_date, cropId: session.crop_id, varietyId: session.variety_id ?? null, stage: "waste", direction: "in", qtyKg: String((d as any).weight_kg) })),
          ]
        : [];
      await client.query("SAVEPOINT before_ledger");
      try {
        await this.syncLedgerInTxn(client, sessionId, header.businessId, gradingEntries);
      } catch (ledgerErr) {
        console.error("[centypack] ledger sync failed on grading create", sessionId, ledgerErr);
        await client.query("ROLLBACK TO SAVEPOINT before_ledger");
      }
      await client.query("COMMIT");
      return { ...session, lines: insertedLines, defects: insertedDefects };
    } catch (err) {
      await client.query("ROLLBACK");
      throw err;
    } finally {
      client.release();
    }
  }

  async updateCentypackGradingSession(
    id: string,
    businessId: string,
    header: Partial<Omit<InsertCentypackGradingSession, "id" | "businessId" | "sessionCode" | "createdAt" | "updatedAt">>,
    lines?: Omit<InsertCentypackGradingLine, "id" | "sessionId" | "businessId" | "createdAt">[],
    defects?: Omit<InsertCentypackGradingDefect, "id" | "sessionId" | "businessId" | "createdAt">[],
  ): Promise<(CentypackGradingSession & { lines: CentypackGradingLine[]; defects: CentypackGradingDefect[] }) | null> {
    const existing = await this.getCentypackGradingSessionById(id, businessId);
    if (!existing) return null;
    const client: PoolClient = await pool.connect();
    try {
      await client.query("BEGIN");
      const { rows: [session] } = await client.query<CentypackGradingSession>(
        `UPDATE centypack_grading_sessions SET session_date=COALESCE($3,session_date), intake_id=COALESCE($4,intake_id), crop_id=COALESCE($5,crop_id), variety_id=COALESCE($6,variety_id), input_weight_kg=COALESCE($7,input_weight_kg), notes=COALESCE($8,notes), status=COALESCE($9,status), updated_at=now() WHERE id=$1 AND business_id=$2 RETURNING *`,
        [id, businessId, header.sessionDate ?? null, header.intakeId ?? null, header.cropId ?? null, header.varietyId ?? null, header.inputWeightKg ?? null, header.notes ?? null, header.status ?? null],
      );
      let updatedLines = existing.lines;
      let updatedDefects = existing.defects;
      if (lines !== undefined) {
        await client.query(`DELETE FROM centypack_grading_lines WHERE session_id=$1`, [id]);
        updatedLines = [];
        for (const line of lines) {
          const { rows: [ins] } = await client.query<CentypackGradingLine>(
            `INSERT INTO centypack_grading_lines (id, session_id, business_id, grade_code_id, net_weight_kg, notes) VALUES ($1,$2,$3,$4,$5,$6) RETURNING *`,
            [crypto.randomUUID(), id, businessId, line.gradeCodeId, line.netWeightKg, line.notes ?? null],
          );
          updatedLines.push(ins);
        }
      }
      if (defects !== undefined) {
        await client.query(`DELETE FROM centypack_grading_defects WHERE session_id=$1`, [id]);
        updatedDefects = [];
        for (const defect of defects) {
          const { rows: [ins] } = await client.query<CentypackGradingDefect>(
            `INSERT INTO centypack_grading_defects (id, session_id, business_id, defect_type_id, weight_kg, notes) VALUES ($1,$2,$3,$4,$5,$6) RETURNING *`,
            [crypto.randomUUID(), id, businessId, defect.defectTypeId, defect.weightKg, defect.notes ?? null],
          );
          updatedDefects.push(ins);
        }
      }
      const gradingUpdateEntries = session.status === "completed"
        ? [
            { txnType: "grading_out", txnDate: session.session_date, cropId: session.crop_id, varietyId: session.variety_id ?? null, stage: "raw", direction: "out", qtyKg: String(session.input_weight_kg) },
            ...updatedLines.map(l => ({ txnType: "grading_in", txnDate: session.session_date, cropId: session.crop_id, varietyId: session.variety_id ?? null, gradeCodeId: (l as any).grade_code_id ?? null, stage: "graded", direction: "in", qtyKg: String((l as any).net_weight_kg) })),
            ...updatedDefects.map(d => ({ txnType: "grading_waste", txnDate: session.session_date, cropId: session.crop_id, varietyId: session.variety_id ?? null, stage: "waste", direction: "in", qtyKg: String((d as any).weight_kg) })),
          ]
        : [];
      await client.query("SAVEPOINT before_ledger");
      try {
        await this.syncLedgerInTxn(client, id, businessId, gradingUpdateEntries);
      } catch (ledgerErr) {
        console.error("[centypack] ledger sync failed on grading update", id, ledgerErr);
        await client.query("ROLLBACK TO SAVEPOINT before_ledger");
      }
      await client.query("COMMIT");
      return { ...session, lines: updatedLines, defects: updatedDefects };
    } catch (err) {
      await client.query("ROLLBACK");
      throw err;
    } finally {
      client.release();
    }
  }
  // CentyPack pack sessions
  async nextCentypackPackCode(businessId: string): Promise<string> {
    const [row] = await db.select({ sessionCode: centypackPackSessions.sessionCode })
      .from(centypackPackSessions).where(eq(centypackPackSessions.businessId, businessId))
      .orderBy(sql`${centypackPackSessions.sessionCode} desc`).limit(1);
    if (!row) return "PSN-0001";
    const last = parseInt(row.sessionCode.replace("PSN-", ""), 10) || 0;
    return `PSN-${String(last + 1).padStart(4, "0")}`;
  }

  async listCentypackPackSessions(businessId: string, opts: { search?: string; cropId?: string; status?: string; dateFrom?: string; dateTo?: string; limit?: number; offset?: number } = {}): Promise<{ rows: (CentypackPackSession & { totalCartons: number; totalWeightKg: number })[]; total: number }> {
    const { search, cropId, status, dateFrom, dateTo, limit = 50, offset = 0 } = opts;
    const conditions: SQL[] = [eq(centypackPackSessions.businessId, businessId)];
    if (cropId) conditions.push(eq(centypackPackSessions.cropId, cropId));
    if (status) conditions.push(eq(centypackPackSessions.status, status));
    if (dateFrom) conditions.push(gte(centypackPackSessions.sessionDate, dateFrom));
    if (dateTo) conditions.push(lte(centypackPackSessions.sessionDate, dateTo));
    if (search) {
      const pattern = `%${search}%`;
      conditions.push(or(
        ilike(centypackPackSessions.sessionCode, pattern),
        ilike(centypackPackSessions.notes, pattern),
      ) as SQL);
    }
    const where = and(...conditions);
    const [{ total }] = await db.select({ total: sql<number>`count(*)::int` }).from(centypackPackSessions).where(where);
    const rows = await db
      .select({
        id: centypackPackSessions.id,
        businessId: centypackPackSessions.businessId,
        sessionCode: centypackPackSessions.sessionCode,
        sessionDate: centypackPackSessions.sessionDate,
        gradingSessionId: centypackPackSessions.gradingSessionId,
        intakeId: centypackPackSessions.intakeId,
        cropId: centypackPackSessions.cropId,
        varietyId: centypackPackSessions.varietyId,
        notes: centypackPackSessions.notes,
        status: centypackPackSessions.status,
        createdAt: centypackPackSessions.createdAt,
        updatedAt: centypackPackSessions.updatedAt,
        totalCartons: sql<number>`coalesce((SELECT sum(pl.carton_count) FROM centypack_pack_lines pl WHERE pl.session_id = ${centypackPackSessions.id}), 0)::int`,
        totalWeightKg: sql<number>`coalesce((SELECT sum(pl.net_weight_kg::numeric) FROM centypack_pack_lines pl WHERE pl.session_id = ${centypackPackSessions.id}), 0)::float`,
      })
      .from(centypackPackSessions)
      .where(where)
      .orderBy(sql`${centypackPackSessions.sessionDate} desc, ${centypackPackSessions.sessionCode} desc`)
      .limit(limit).offset(offset);
    return { rows: rows as (CentypackPackSession & { totalCartons: number; totalWeightKg: number })[], total: total ?? 0 };
  }

  async getCentypackPackSessionById(id: string, businessId: string): Promise<(CentypackPackSession & { lines: (CentypackPackLine & { labourers: (CentypackPackLineLabourer & { labourerCode: string; firstName: string; lastName: string })[] })[] }) | null> {
    const [header] = await db.select().from(centypackPackSessions).where(and(eq(centypackPackSessions.id, id), eq(centypackPackSessions.businessId, businessId))).limit(1);
    if (!header) return null;
    const lines = await db.select().from(centypackPackLines).where(eq(centypackPackLines.sessionId, id));
    const linesWithLabourers = await Promise.all(lines.map(async (line) => {
      const labourers = await this.getPackLineLabourers(line.id);
      return { ...line, labourers };
    }));
    return { ...header, lines: linesWithLabourers };
  }

  async createCentypackPackSession(
    header: Omit<InsertCentypackPackSession, "id" | "createdAt" | "updatedAt">,
    lines: (Omit<InsertCentypackPackLine, "id" | "sessionId" | "businessId" | "createdAt"> & { labourers?: { labourerId: string; cartonCount: number }[] })[],
  ): Promise<CentypackPackSession & { lines: CentypackPackLine[] }> {
    const sessionId = crypto.randomUUID();
    const client: PoolClient = await pool.connect();
    try {
      await client.query("BEGIN");
      const { rows: [session] } = await client.query<CentypackPackSession>(
        `INSERT INTO centypack_pack_sessions (id, business_id, session_code, session_date, grading_session_id, intake_id, crop_id, variety_id, notes, status)
         VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10) RETURNING *`,
        [sessionId, header.businessId, header.sessionCode, header.sessionDate, header.gradingSessionId ?? null, header.intakeId ?? null, header.cropId, header.varietyId ?? null, header.notes ?? null, header.status ?? "draft"],
      );
      const insertedLines: CentypackPackLine[] = [];
      for (const line of lines) {
        const lineId = crypto.randomUUID();
        const { rows: [ins] } = await client.query<CentypackPackLine>(
          `INSERT INTO centypack_pack_lines (id, session_id, business_id, grade_code_id, carton_type_id, carton_count, net_weight_kg, notes) VALUES ($1,$2,$3,$4,$5,$6,$7,$8) RETURNING *`,
          [lineId, sessionId, header.businessId, line.gradeCodeId ?? null, line.cartonTypeId ?? null, line.cartonCount ?? null, line.netWeightKg, line.notes ?? null],
        );
        insertedLines.push(ins);
        if (line.labourers?.length) {
          for (const la of line.labourers) {
            await client.query(
              `INSERT INTO centypack_pack_line_labourers (id, pack_line_id, labourer_id, carton_count) VALUES ($1,$2,$3,$4) ON CONFLICT (pack_line_id, labourer_id) DO UPDATE SET carton_count=EXCLUDED.carton_count`,
              [crypto.randomUUID(), lineId, la.labourerId, la.cartonCount],
            );
          }
        }
      }
      const packCreateEntries = session.status === "completed"
        ? insertedLines.flatMap(l => [
            { txnType: "pack_out", txnDate: session.session_date, cropId: session.crop_id, varietyId: session.variety_id ?? null, gradeCodeId: (l as any).grade_code_id ?? null, stage: "graded", direction: "out", qtyKg: String((l as any).net_weight_kg) },
            { txnType: "pack_in",  txnDate: session.session_date, cropId: session.crop_id, varietyId: session.variety_id ?? null, gradeCodeId: (l as any).grade_code_id ?? null, cartonTypeId: (l as any).carton_type_id ?? null, stage: "packed", direction: "in", qtyKg: String((l as any).net_weight_kg), cartonCount: (l as any).carton_count ?? null },
          ])
        : [];
      await client.query("SAVEPOINT before_ledger");
      try {
        await this.syncLedgerInTxn(client, sessionId, header.businessId, packCreateEntries);
      } catch (ledgerErr) {
        console.error("[centypack] ledger sync failed on pack create", sessionId, ledgerErr);
        await client.query("ROLLBACK TO SAVEPOINT before_ledger");
      }
      await client.query("COMMIT");
      return { ...session, lines: insertedLines };
    } catch (err) {
      await client.query("ROLLBACK");
      throw err;
    } finally {
      client.release();
    }
  }

  async updateCentypackPackSession(
    id: string,
    businessId: string,
    header: Partial<Omit<InsertCentypackPackSession, "id" | "businessId" | "sessionCode" | "createdAt" | "updatedAt">>,
    lines?: (Omit<InsertCentypackPackLine, "id" | "sessionId" | "businessId" | "createdAt"> & { labourers?: { labourerId: string; cartonCount: number }[] })[],
    lineLabourers?: Record<string, { labourerId: string; cartonCount: number }[]>,
  ): Promise<(CentypackPackSession & { lines: CentypackPackLine[] }) | null> {
    const existing = await this.getCentypackPackSessionById(id, businessId);
    if (!existing) return null;
    const client: PoolClient = await pool.connect();
    try {
      await client.query("BEGIN");
      const { rows: [session] } = await client.query<CentypackPackSession>(
        `UPDATE centypack_pack_sessions SET session_date=COALESCE($3,session_date), grading_session_id=COALESCE($4,grading_session_id), intake_id=COALESCE($5,intake_id), crop_id=COALESCE($6,crop_id), variety_id=COALESCE($7,variety_id), notes=COALESCE($8,notes), status=COALESCE($9,status), updated_at=now() WHERE id=$1 AND business_id=$2 RETURNING *`,
        [id, businessId, header.sessionDate ?? null, header.gradingSessionId ?? null, header.intakeId ?? null, header.cropId ?? null, header.varietyId ?? null, header.notes ?? null, header.status ?? null],
      );
      let updatedLines: CentypackPackLine[] = existing.lines;
      if (lines !== undefined) {
        await client.query(`DELETE FROM centypack_pack_lines WHERE session_id=$1`, [id]);
        updatedLines = [];
        for (const line of lines) {
          const lineId = crypto.randomUUID();
          const { rows: [ins] } = await client.query<CentypackPackLine>(
            `INSERT INTO centypack_pack_lines (id, session_id, business_id, grade_code_id, carton_type_id, carton_count, net_weight_kg, notes) VALUES ($1,$2,$3,$4,$5,$6,$7,$8) RETURNING *`,
            [lineId, id, businessId, line.gradeCodeId ?? null, line.cartonTypeId ?? null, line.cartonCount ?? null, line.netWeightKg, line.notes ?? null],
          );
          updatedLines.push(ins);
          if (line.labourers?.length) {
            for (const la of line.labourers) {
              await client.query(
                `INSERT INTO centypack_pack_line_labourers (id, pack_line_id, labourer_id, carton_count) VALUES ($1,$2,$3,$4) ON CONFLICT (pack_line_id, labourer_id) DO UPDATE SET carton_count=EXCLUDED.carton_count`,
                [crypto.randomUUID(), lineId, la.labourerId, la.cartonCount],
              );
            }
          }
        }
      }
      const packUpdateEntries = session.status === "completed"
        ? updatedLines.flatMap(l => [
            { txnType: "pack_out", txnDate: session.session_date, cropId: session.crop_id, varietyId: session.variety_id ?? null, gradeCodeId: (l as any).grade_code_id ?? null, stage: "graded", direction: "out", qtyKg: String((l as any).net_weight_kg) },
            { txnType: "pack_in",  txnDate: session.session_date, cropId: session.crop_id, varietyId: session.variety_id ?? null, gradeCodeId: (l as any).grade_code_id ?? null, cartonTypeId: (l as any).carton_type_id ?? null, stage: "packed", direction: "in", qtyKg: String((l as any).net_weight_kg), cartonCount: (l as any).carton_count ?? null },
          ])
        : [];
      await client.query("SAVEPOINT before_ledger");
      try {
        await this.syncLedgerInTxn(client, id, businessId, packUpdateEntries);
      } catch (ledgerErr) {
        console.error("[centypack] ledger sync failed on pack update", id, ledgerErr);
        await client.query("ROLLBACK TO SAVEPOINT before_ledger");
      }
      await client.query("COMMIT");
      return { ...session, lines: updatedLines };
    } catch (err) {
      await client.query("ROLLBACK");
      throw err;
    } finally {
      client.release();
    }
  }

  async nextCentypackLabourerCode(businessId: string): Promise<string> {
    const [row] = await db.select({ labourerCode: centypackLabourers.labourerCode })
      .from(centypackLabourers).where(eq(centypackLabourers.businessId, businessId))
      .orderBy(sql`${centypackLabourers.labourerCode} desc`).limit(1);
    if (!row) return "LBR-0001";
    const last = parseInt(row.labourerCode.replace("LBR-", ""), 10) || 0;
    return `LBR-${String(last + 1).padStart(4, "0")}`;
  }

  async listCentypackLabourers(businessId: string, opts: { search?: string; status?: string; workerCategoryId?: string; limit?: number; offset?: number } = {}): Promise<{ rows: (CentypackLabourer & { workerCategoryName: string | null })[]; total: number }> {
    const { search, status, workerCategoryId, limit = 100, offset = 0 } = opts;
    const conditions: SQL[] = [eq(centypackLabourers.businessId, businessId)];
    if (status) conditions.push(eq(centypackLabourers.status, status));
    if (workerCategoryId) conditions.push(eq(centypackLabourers.workerCategoryId, workerCategoryId));
    if (search) {
      const pattern = `%${search}%`;
      conditions.push(or(
        ilike(centypackLabourers.firstName, pattern),
        ilike(centypackLabourers.lastName, pattern),
        ilike(centypackLabourers.labourerCode, pattern),
        ilike(centypackLabourers.phone, pattern),
      ) as SQL);
    }
    const where = and(...conditions);
    const [{ total }] = await db.select({ total: sql<number>`count(*)::int` }).from(centypackLabourers).where(where);
    const rows = await db
      .select({
        id: centypackLabourers.id,
        businessId: centypackLabourers.businessId,
        labourerCode: centypackLabourers.labourerCode,
        firstName: centypackLabourers.firstName,
        lastName: centypackLabourers.lastName,
        phone: centypackLabourers.phone,
        workerCategoryId: centypackLabourers.workerCategoryId,
        status: centypackLabourers.status,
        notes: centypackLabourers.notes,
        createdAt: centypackLabourers.createdAt,
        updatedAt: centypackLabourers.updatedAt,
        workerCategoryName: sql<string | null>`(SELECT name FROM centypack_worker_categories WHERE id = ${centypackLabourers.workerCategoryId})`,
      })
      .from(centypackLabourers)
      .where(where)
      .orderBy(centypackLabourers.labourerCode)
      .limit(limit).offset(offset);
    return { rows: rows as (CentypackLabourer & { workerCategoryName: string | null })[], total: total ?? 0 };
  }

  async getCentypackLabourerById(id: string, businessId: string): Promise<CentypackLabourer | null> {
    const [row] = await db.select().from(centypackLabourers).where(and(eq(centypackLabourers.id, id), eq(centypackLabourers.businessId, businessId))).limit(1);
    return row ?? null;
  }

  async createCentypackLabourer(data: Omit<InsertCentypackLabourer, "id" | "createdAt" | "updatedAt">): Promise<CentypackLabourer> {
    const [row] = await db.insert(centypackLabourers).values({ ...data, id: crypto.randomUUID() }).returning();
    return row;
  }

  async updateCentypackLabourer(id: string, businessId: string, data: Partial<Omit<InsertCentypackLabourer, "id" | "businessId" | "labourerCode" | "createdAt" | "updatedAt">>): Promise<CentypackLabourer | null> {
    const [row] = await db.update(centypackLabourers)
      .set({ ...data, updatedAt: new Date() })
      .where(and(eq(centypackLabourers.id, id), eq(centypackLabourers.businessId, businessId)))
      .returning();
    return row ?? null;
  }

  async getLabourAttendanceByDate(businessId: string, date: string): Promise<{ labourer: CentypackLabourer & { workerCategoryName: string | null }; attendance: CentypackLabourAttendance | null }[]> {
    const { rows: labourers } = await this.listCentypackLabourers(businessId, { status: "active", limit: 1000 });
    const attendanceRows = await db.select().from(centypackLabourAttendance)
      .where(and(eq(centypackLabourAttendance.businessId, businessId), eq(centypackLabourAttendance.attendanceDate, date)));
    const attMap = new Map(attendanceRows.map(a => [a.labourerId, a]));
    return labourers.map(l => ({ labourer: l, attendance: attMap.get(l.id) ?? null }));
  }

  async upsertLabourAttendance(businessId: string, date: string, records: { labourerId: string; status: string; notes?: string | null }[]): Promise<void> {
    const client: PoolClient = await pool.connect();
    try {
      await client.query("BEGIN");
      for (const rec of records) {
        await client.query(
          `INSERT INTO centypack_labour_attendance (id, business_id, labourer_id, attendance_date, status, notes)
           VALUES ($1,$2,$3,$4,$5,$6)
           ON CONFLICT (business_id, attendance_date, labourer_id)
           DO UPDATE SET status=EXCLUDED.status, notes=EXCLUDED.notes, updated_at=now()`,
          [crypto.randomUUID(), businessId, rec.labourerId, date, rec.status, rec.notes ?? null],
        );
      }
      await client.query("COMMIT");
    } catch (err) {
      await client.query("ROLLBACK");
      throw err;
    } finally {
      client.release();
    }
  }

  async getLabourPackSummary(businessId: string, opts: { dateFrom?: string; dateTo?: string; labourerId?: string; packSessionId?: string } = {}): Promise<{ labourerId: string; labourerCode: string; firstName: string; lastName: string; packSessionId: string; packSessionCode: string; sessionDate: string; packLineId: string; gradeCodeId: string | null; cartonCount: number }[]> {
    const { dateFrom, dateTo, labourerId, packSessionId } = opts;
    let q = `
      SELECT
        lb.id           AS "labourerId",
        lb.labourer_code AS "labourerCode",
        lb.first_name   AS "firstName",
        lb.last_name    AS "lastName",
        ps.id           AS "packSessionId",
        ps.session_code AS "packSessionCode",
        ps.session_date AS "sessionDate",
        pl.id           AS "packLineId",
        pl.grade_code_id AS "gradeCodeId",
        pll.carton_count AS "cartonCount"
      FROM centypack_pack_line_labourers pll
      JOIN centypack_labourers lb  ON lb.id = pll.labourer_id
      JOIN centypack_pack_lines pl ON pl.id = pll.pack_line_id
      JOIN centypack_pack_sessions ps ON ps.id = pl.session_id
      WHERE lb.business_id = $1
    `;
    const params: unknown[] = [businessId];
    let i = 2;
    if (dateFrom) { q += ` AND ps.session_date >= $${i++}`; params.push(dateFrom); }
    if (dateTo)   { q += ` AND ps.session_date <= $${i++}`; params.push(dateTo); }
    if (labourerId) { q += ` AND lb.id = $${i++}`; params.push(labourerId); }
    if (packSessionId) { q += ` AND ps.id = $${i++}`; params.push(packSessionId); }
    q += ` ORDER BY ps.session_date DESC, lb.labourer_code`;
    const { rows } = await pool.query(q, params);
    return rows;
  }

  async getPackLineLabourers(packLineId: string): Promise<(CentypackPackLineLabourer & { labourerCode: string; firstName: string; lastName: string })[]> {
    const rows = await pool.query<CentypackPackLineLabourer & { labourerCode: string; firstName: string; lastName: string }>(
      `SELECT pll.*, lb.labourer_code AS "labourerCode", lb.first_name AS "firstName", lb.last_name AS "lastName"
       FROM centypack_pack_line_labourers pll
       JOIN centypack_labourers lb ON lb.id = pll.labourer_id
       WHERE pll.pack_line_id = $1
       ORDER BY lb.labourer_code`,
      [packLineId],
    );
    return rows.rows;
  }

  async upsertPackLineLabourers(packLineId: string, businessId: string, assignments: { labourerId: string; cartonCount: number }[]): Promise<void> {
    const client: PoolClient = await pool.connect();
    try {
      await client.query("BEGIN");
      await client.query(`DELETE FROM centypack_pack_line_labourers WHERE pack_line_id = $1`, [packLineId]);
      for (const a of assignments) {
        await client.query(
          `INSERT INTO centypack_pack_line_labourers (id, pack_line_id, labourer_id, carton_count) VALUES ($1,$2,$3,$4)`,
          [crypto.randomUUID(), packLineId, a.labourerId, a.cartonCount],
        );
      }
      await client.query("COMMIT");
    } catch (err) {
      await client.query("ROLLBACK");
      throw err;
    } finally {
      client.release();
    }
  }

  // ── Stock ledger ─────────────────────────────────────────────────────────────

  private async syncLedgerInTxn(
    client: PoolClient,
    txnId: string,
    businessId: string,
    entries: Array<{
      txnType: string; txnDate: string; cropId: string;
      varietyId?: string | null; gradeCodeId?: string | null;
      cartonTypeId?: string | null; warehouseId?: string | null;
      stage: string; direction: string;
      qtyKg?: string | null; cartonCount?: number | null;
    }>,
  ): Promise<void> {
    await client.query(`DELETE FROM centypack_stock_ledger WHERE txn_id=$1 AND business_id=$2`, [txnId, businessId]);
    for (const e of entries) {
      await client.query(
        `INSERT INTO centypack_stock_ledger (id,business_id,txn_type,txn_id,txn_date,crop_id,variety_id,grade_code_id,carton_type_id,warehouse_id,stage,direction,qty_kg,carton_count)
         VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14)`,
        [crypto.randomUUID(), businessId, e.txnType, txnId, e.txnDate, e.cropId,
         e.varietyId ?? null, e.gradeCodeId ?? null, e.cartonTypeId ?? null, e.warehouseId ?? null,
         e.stage, e.direction, e.qtyKg ?? null, e.cartonCount ?? null],
      );
    }
  }

  async getStockOnHand(businessId: string, opts: { cropId?: string; stage?: string; warehouseId?: string } = {}): Promise<StockOnHandRow[]> {
    const { cropId, stage, warehouseId } = opts;
    const conditions = [`sl.business_id = $1`, `sl.stage != 'waste'`];
    const params: unknown[] = [businessId];
    if (cropId) { params.push(cropId); conditions.push(`sl.crop_id = $${params.length}`); }
    if (stage) { params.push(stage); conditions.push(`sl.stage = $${params.length}`); }
    if (warehouseId) { params.push(warehouseId); conditions.push(`sl.warehouse_id = $${params.length}`); }
    const where = conditions.join(" AND ");
    const { rows } = await pool.query<StockOnHandRow & { qty_kg_balance: string; carton_balance: string }>(
      `SELECT
         sl.crop_id        AS "cropId",
         COALESCE(c.name, sl.crop_id)  AS "cropName",
         sl.variety_id     AS "varietyId",
         v.name            AS "varietyName",
         sl.grade_code_id  AS "gradeCodeId",
         g.name            AS "gradeCodeName",
         sl.carton_type_id AS "cartonTypeId",
         ct.name           AS "cartonTypeName",
         sl.warehouse_id   AS "warehouseId",
         w.name            AS "warehouseName",
         sl.stage,
         SUM(CASE WHEN sl.direction='in' THEN sl.qty_kg::numeric ELSE -sl.qty_kg::numeric END)          AS qty_kg_balance,
         SUM(CASE WHEN sl.direction='in' THEN COALESCE(sl.carton_count,0) ELSE -COALESCE(sl.carton_count,0) END) AS carton_balance
       FROM centypack_stock_ledger sl
       LEFT JOIN centypack_crops    c  ON c.id  = sl.crop_id
       LEFT JOIN centypack_varieties v  ON v.id  = sl.variety_id
       LEFT JOIN centypack_grade_codes g ON g.id = sl.grade_code_id
       LEFT JOIN centypack_carton_types ct ON ct.id = sl.carton_type_id
       LEFT JOIN centypack_warehouses   w  ON w.id  = sl.warehouse_id
       WHERE ${where}
       GROUP BY sl.crop_id, c.name, sl.variety_id, v.name,
                sl.grade_code_id, g.name, sl.carton_type_id, ct.name,
                sl.warehouse_id, w.name, sl.stage
       HAVING SUM(CASE WHEN sl.direction='in' THEN sl.qty_kg::numeric ELSE -sl.qty_kg::numeric END) > 0.001
           OR SUM(CASE WHEN sl.direction='in' THEN COALESCE(sl.carton_count,0) ELSE -COALESCE(sl.carton_count,0) END) > 0
       ORDER BY sl.stage, "cropName"`,
      params,
    );
    return rows.map(r => ({
      ...r,
      qtyKgBalance: parseFloat((r as any).qty_kg_balance ?? "0"),
      cartonBalance: parseInt(String((r as any).carton_balance ?? "0"), 10),
    }));
  }

  async getStockMovements(businessId: string, opts: { txnId?: string; txnType?: string; cropId?: string; stage?: string; dateFrom?: string; dateTo?: string; limit?: number; offset?: number } = {}): Promise<{ rows: StockMovementRow[]; total: number }> {
    const { txnId, txnType, cropId, stage, dateFrom, dateTo, limit = 100, offset = 0 } = opts;
    const conditions = [`sl.business_id = $1`];
    const params: unknown[] = [businessId];
    if (txnId)   { params.push(txnId);   conditions.push(`sl.txn_id = $${params.length}`); }
    if (txnType) { params.push(txnType); conditions.push(`sl.txn_type = $${params.length}`); }
    if (cropId)  { params.push(cropId);  conditions.push(`sl.crop_id = $${params.length}`); }
    if (stage)   { params.push(stage);   conditions.push(`sl.stage = $${params.length}`); }
    if (dateFrom){ params.push(dateFrom);conditions.push(`sl.txn_date >= $${params.length}`); }
    if (dateTo)  { params.push(dateTo);  conditions.push(`sl.txn_date <= $${params.length}`); }
    const where = conditions.join(" AND ");
    const countRes = await pool.query<{ total: string }>(`SELECT count(*)::text AS total FROM centypack_stock_ledger sl WHERE ${where}`, params);
    const total = parseInt(countRes.rows[0]?.total ?? "0", 10);
    params.push(limit, offset);
    const { rows } = await pool.query(
      `SELECT sl.*,
         COALESCE(c.name, sl.crop_id) AS "cropName",
         v.name  AS "varietyName",
         g.name  AS "gradeCodeName",
         ct.name AS "cartonTypeName",
         w.name  AS "warehouseName"
       FROM centypack_stock_ledger sl
       LEFT JOIN centypack_crops        c  ON c.id  = sl.crop_id
       LEFT JOIN centypack_varieties    v  ON v.id  = sl.variety_id
       LEFT JOIN centypack_grade_codes  g  ON g.id  = sl.grade_code_id
       LEFT JOIN centypack_carton_types ct ON ct.id = sl.carton_type_id
       LEFT JOIN centypack_warehouses   w  ON w.id  = sl.warehouse_id
       WHERE ${where}
       ORDER BY sl.txn_date DESC, sl.created_at DESC
       LIMIT $${params.length - 1} OFFSET $${params.length}`,
      params,
    );
    return { rows: rows as StockMovementRow[], total };
  }
}

export const storage = new DatabaseStorage();
