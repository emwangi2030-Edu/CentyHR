const KRA_PIN_REGEX = /^[A-Z][0-9]{9}[A-Z]$/;

export function normalizePhone(input: string): string {
  const digits = input.replace(/\D+/g, "");
  if (digits.startsWith("254") && digits.length >= 12) return `+${digits.slice(0, 12)}`;
  if (digits.startsWith("0") && digits.length === 10) return `+254${digits.slice(1)}`;
  if (digits.length === 9) return `+254${digits}`;
  return input.trim();
}

export function normalizeEmail(input: string): string {
  return input.trim().toLowerCase();
}

export function normalizeKraPin(input: string): string {
  return input.trim().toUpperCase();
}

export function validateKraPin(pin: string): boolean {
  return KRA_PIN_REGEX.test(normalizeKraPin(pin));
}

export function validateEmployeeDoc(doc: Record<string, unknown>): string[] {
  const errors: string[] = [];
  const push = (msg: string) => errors.push(msg);

  const emailFields = ["prefered_email", "company_email", "personal_email"] as const;
  const emailRegex = /^[^\s@]+@[^\s@]+\.[^\s@]+$/;

  for (const field of emailFields) {
    const v = doc[field];
    if (typeof v === "string" && v.trim() && !emailRegex.test(v.trim())) {
      push(`${field} is invalid`);
    }
  }

  if (typeof doc.cell_number === "string" && doc.cell_number.trim()) {
    const phone = normalizePhone(doc.cell_number);
    if (!/^\+254\d{9}$/.test(phone)) push("cell_number must be a valid Kenyan number");
  }

  if (typeof doc.tax_id === "string" && doc.tax_id.trim()) {
    if (!validateKraPin(doc.tax_id)) push("tax_id must be a valid KRA PIN format");
  }

  return errors;
}
