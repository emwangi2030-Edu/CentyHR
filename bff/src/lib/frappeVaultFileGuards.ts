/**
 * Shared validation for document-vault file references and Frappe static paths.
 * Keeps GET semantics aligned between the HTTP route and erpnext fetchBinary.
 */

/** @returns error message, or null if valid */
export function validateVaultFileDocname(fileName: string): string | null {
  if (fileName.length < 1 || fileName.length > 140) {
    return "Invalid file reference";
  }
  if (!/^[a-zA-Z0-9._-]+$/.test(fileName)) {
    return "Invalid file reference";
  }
  return null;
}

/**
 * Only allow Frappe static file paths from vault attachments — not arbitrary site paths,
 * query strings, or traversal (including %-encoded ..).
 * @returns error message, or null if valid
 */
export function validateFrappeVaultFileUrlPath(fileUrl: string): string | null {
  const u = fileUrl.trim();
  if (!u.startsWith("/") || u.includes("?") || u.includes("#")) {
    return "File has no downloadable path";
  }
  if (u.includes("//") || u.includes("\\") || u.includes("..")) {
    return "File has no downloadable path";
  }
  let decoded: string;
  try {
    decoded = decodeURIComponent(u);
  } catch {
    return "File has no downloadable path";
  }
  if (decoded.includes("..") || decoded.includes("\\") || decoded.includes("//")) {
    return "File has no downloadable path";
  }
  const ok =
    decoded.startsWith("/private/files/") || decoded.startsWith("/files/");
  if (!ok) {
    return "File has no downloadable path";
  }
  return null;
}
