/**
 * Validation and normalisation helpers for Employee documents.
 */

export interface FieldError {
  field: string;
  message: string;
}

// ── Normalisation ─────────────────────────────────────────────────────────────

/**
 * Normalise a Kenyan KRA PIN: uppercase, strip surrounding whitespace.
 * Format: A000000000A (letter, 9 digits, letter).
 */
export function normalizeKraPin(pin: string): string {
  return pin.trim().toUpperCase();
}

/**
 * Normalise a Kenyan phone number to the +254XXXXXXXXX format.
 * Accepts 07XXXXXXXX, 01XXXXXXXX, 2547XXXXXXXX, +2547XXXXXXXX.
 */
export function normalizePhone(phone: string): string {
  const digits = phone.replace(/\D/g, "");
  if (digits.startsWith("254") && digits.length === 12) {
    return `+${digits}`;
  }
  if (digits.startsWith("0") && digits.length === 10) {
    return `+254${digits.slice(1)}`;
  }
  // Already has + prefix from original
  if (phone.trim().startsWith("+") && digits.length === 12) {
    return `+${digits}`;
  }
  return phone.trim();
}

/**
 * Normalise an email address: lowercase and trim.
 */
export function normalizeEmail(email: string): string {
  return email.trim().toLowerCase();
}

// ── Validators ────────────────────────────────────────────────────────────────

/**
 * Returns true if the string is a valid Kenyan KRA PIN (A000000000A).
 */
export function validateKraPin(pin: string): boolean {
  return /^[A-Z]\d{9}[A-Z]$/.test(normalizeKraPin(pin));
}

/**
 * Validate fields on an Employee document (partial or full).
 * Returns an array of field errors; empty array means valid.
 */
export function validateEmployeeDoc(
  doc: Record<string, unknown>
): FieldError[] {
  const errors: FieldError[] = [];

  // KRA PIN format
  if (doc.tax_id) {
    const pin = String(doc.tax_id);
    if (!validateKraPin(pin)) {
      errors.push({
        field: "tax_id",
        message: `KRA PIN "${pin}" is not valid. Expected format: A000000000A (letter, 9 digits, letter).`,
      });
    }
  }

  // Phone number
  if (doc.cell_number) {
    const phone = String(doc.cell_number);
    if (!/^\+254\d{9}$/.test(phone)) {
      errors.push({
        field: "cell_number",
        message: `Phone number "${phone}" is not valid. Expected format: +254XXXXXXXXX.`,
      });
    }
  }

  // Email fields
  for (const field of ["prefered_email", "company_email", "personal_email"] as const) {
    if (doc[field]) {
      const email = String(doc[field]);
      if (!/^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(email)) {
        errors.push({
          field,
          message: `"${email}" is not a valid email address.`,
        });
      }
    }
  }

  // National ID (numeric, 5–10 digits)
  if (doc.id_number) {
    const id = String(doc.id_number).trim();
    if (!/^\d{5,10}$/.test(id)) {
      errors.push({
        field: "id_number",
        message: `National ID "${id}" is not valid. Expected 5–10 digits.`,
      });
    }
  }

  return errors;
}
