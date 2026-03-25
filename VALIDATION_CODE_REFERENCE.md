# Form Validation Code Reference

## Complete Code Locations

### 1. Frontend Validation Functions (index.html, lines 9579-9811)

#### Budget Validation
```javascript
function validateBudgetInput(budgetValue) {
  if (!budgetValue) {
    return { valid: false, error: "Budget is required" };
  }

  // Parse budget string (handles formats like $50,000, 50K, 50000, etc)
  const budgetStr = budgetValue.replace(/[\$,]/g, "").trim();
  let amount = 0;

  // Handle K, M, B suffixes
  if (/^\d+\.?\d*[KMB]$/.test(budgetStr.toUpperCase())) {
    const num = parseFloat(budgetStr);
    const suffix = budgetStr.charAt(budgetStr.length - 1).toUpperCase();
    if (suffix === "K") amount = num * 1000;
    else if (suffix === "M") amount = num * 1000000;
    else if (suffix === "B") amount = num * 1000000000;
  } else {
    amount = parseFloat(budgetStr);
  }

  if (isNaN(amount) || amount <= 0) {
    return { valid: false, error: "Budget must be greater than 0" };
  }
  if (amount > 1000000000) {
    return { valid: false, error: "Budget exceeds maximum allowed value ($1B)" };
  }
  return { valid: true, error: "" };
}
```

#### Location Validation
```javascript
function validateLocations(locationList) {
  const errors = [];

  if (!locationList || locationList.length === 0) {
    errors.push("At least one location is required");
    return { valid: false, errors };
  }

  // Invalid location patterns
  const invalidPatterns = [
    /^hell$/i,
    /^test$/i,
    /^xxx$/i,
    /^fake$/i,
    /^placeholder$/i,
    /^n\/a$/i,
    /^\d+$/,  // only numbers
    /^[^a-z0-9\s,\-\.]*$/i  // no alphanumeric
  ];

  locationList.forEach((loc, idx) => {
    const normalized = loc.trim().toUpperCase();

    // Check against invalid patterns
    if (invalidPatterns.some(p => p.test(loc))) {
      errors.push(`Location "${loc}" is not valid`);
      return;
    }

    // Check if it looks like a real location (at least 2 characters, contains letters)
    if (loc.length < 2 || !/[a-zA-Z]/.test(loc)) {
      errors.push(`Location "${loc}" is too short or invalid`);
      return;
    }

    // Try to match against known countries/states
    const isValidLocation = VALID_US_STATES.has(normalized.split(/[\s,]+/)[0]) ||
                           VALID_COUNTRIES.has(normalized.split(/[\s,]+/)[0]) ||
                           /^[a-zA-Z\s,\-\.]{2,}$/.test(loc);

    if (!isValidLocation && !(/[a-z]{3,}/i.test(loc))) {
      errors.push(`Location "${loc}" appears invalid (should be city/state/country)`);
    }
  });

  return { valid: errors.length === 0, errors };
}
```

#### Hire Volume Validation
```javascript
function validateHireVolume(hireVolumeStr) {
  if (!hireVolumeStr) {
    return { valid: true, error: "" };  // Optional field
  }

  // Parse hire volume (handles "100", "100 hires", "100-500", etc)
  const match = hireVolumeStr.match(/(\d+)/);
  if (!match) {
    return { valid: false, error: "Hire volume must be a number greater than 0" };
  }

  const num = parseInt(match[1], 10);
  if (num <= 0) {
    return { valid: false, error: "Hire volume must be greater than 0" };
  }
  if (num > 100000) {
    return { valid: false, error: "Hire volume exceeds maximum (100,000)" };
  }

  return { valid: true, error: "" };
}
```

#### Competitor Validation
```javascript
function validateCompetitors(competitorList) {
  const errors = [];

  competitorList.forEach((comp) => {
    const normalized = comp.trim();
    if (normalized.length < 2) {
      errors.push(`Competitor "${comp}" is too short`);
    } else if (!/[a-zA-Z]/.test(normalized)) {
      errors.push(`Competitor "${comp}" must contain letters`);
    }
  });

  return { valid: errors.length === 0, errors };
}
```

#### Error Display Helper
```javascript
function showFormError(fieldId, errorMsg) {
  if (fieldId) {
    const field = document.getElementById(fieldId);
    if (field) {
      const formGroup = field.closest(".form-group");
      if (formGroup) {
        formGroup.classList.add("field-error");
        formGroup.classList.remove("field-valid");

        // Find or create error message element
        let errorEl = formGroup.querySelector(".field-error-msg");
        if (!errorEl) {
          errorEl = document.createElement("div");
          errorEl.className = "field-error-msg";
          formGroup.appendChild(errorEl);
        }
        errorEl.textContent = errorMsg;
        errorEl.style.display = "block";

        // Trigger shake animation
        formGroup.style.animation = "none";
        setTimeout(() => {
          formGroup.style.animation = "shake 0.4s ease";
        }, 10);
      }
    }
  }
  showToast(errorMsg, "error");
}
```

#### Valid Locations Lists
```javascript
const VALID_US_STATES = new Set([
  "AL", "AK", "AZ", "AR", "CA", "CO", "CT", "DE", "FL", "GA", "HI", "ID", "IL", "IN", "IA", "KS", "KY", "LA", "ME", "MD", "MA", "MI", "MN", "MS", "MO", "MT", "NE", "NV", "NH", "NJ", "NM", "NY", "NC", "ND", "OH", "OK", "OR", "PA", "RI", "SC", "SD", "TN", "TX", "UT", "VT", "VA", "WA", "WV", "WI", "WY", "DC", "PR", "VI", "GU", "AS", "MP"
]);

const VALID_COUNTRIES = new Set([
  "UNITED STATES", "USA", "US", "CANADA", "MEXICO", "UNITED KINGDOM", "UK", "ENGLAND", "IRELAND", "FRANCE", "GERMANY", "NETHERLANDS", "BELGIUM", "ITALY", "SPAIN", "PORTUGAL", "SWITZERLAND", "AUSTRIA", "SWEDEN", "NORWAY", "DENMARK", "FINLAND", "AUSTRALIA", "NEW ZEALAND", "SINGAPORE", "MALAYSIA", "INDIA", "JAPAN", "SOUTH KOREA", "KOREA", "CHINA", "HONG KONG", "PHILIPPINES", "THAILAND", "VIETNAM", "BRAZIL", "ARGENTINA", "CHILE", "COLOMBIA", "PERU", "SOUTH AFRICA", "UAE", "AUSTRALIA", "INDIA"
]);
```

#### Integration into generatePlan()
```javascript
async function generatePlan() {
  // ... existing name/email validation ...

  // ── CRITICAL: Validate budget ──
  const budgetRangeSelect = document.getElementById("budgetRange");
  const budgetValue = budgetRangeSelect.value === "__exact__"
    ? document.getElementById("exactBudget").value.trim()
    : budgetRangeSelect.value;

  const budgetValidation = validateBudgetInput(budgetValue);
  if (!budgetValidation.valid) {
    showFormError("budgetRange", budgetValidation.error);
    return;
  }

  // ── CRITICAL: Validate locations ──
  if (locations.length === 0) {
    showFormError("locationInput", "At least one location is required");
    const locContainer = document.getElementById("locationsContainer");
    if (locContainer) {
      locContainer.classList.add("field-error");
      locContainer.style.animation = "shake 0.4s ease";
    }
    return;
  }

  const locValidation = validateLocations(locations);
  if (!locValidation.valid) {
    const errorMsg = locValidation.errors[0] || "Invalid location(s)";
    showFormError("locationInput", errorMsg);
    const locContainer = document.getElementById("locationsContainer");
    if (locContainer) {
      locContainer.classList.add("field-error");
      locContainer.style.animation = "shake 0.4s ease";
    }
    return;
  }

  // ── CRITICAL: Validate hire volume if provided ──
  const hireVolSelect = document.getElementById("hireVolume");
  const hireVolValue = hireVolSelect.value === "__exact__"
    ? document.getElementById("exactHires").value.trim()
    : hireVolSelect.value;

  if (hireVolValue) {
    const hireValidation = validateHireVolume(hireVolValue);
    if (!hireValidation.valid) {
      showFormError("hireVolume", hireValidation.error);
      return;
    }
  }

  // Validate competitors if provided
  if (competitors.length > 0) {
    const compValidation = validateCompetitors(competitors);
    if (!compValidation.valid) {
      const errorMsg = compValidation.errors[0] || "Invalid competitor(s)";
      showFormError("competitorInput", errorMsg);
      return;
    }
  }

  // ... rest of generatePlan logic ...
}
```

---

### 2. HTML Error Message Elements

**Location: `/templates/index.html`**

```html
<!-- Budget field error (line 5147) -->
<div class="copilot-nudge"
  id="nudge-budget"
  aria-live="polite"
  aria-label="Budget insight"
></div>
<div class="field-error-msg" id="budgetRange-error"></div>

<!-- Location field error (line 5320) -->
<div class="copilot-nudge"
  id="nudge-location"
  aria-live="polite"
  aria-label="Location insight"
></div>
<div class="field-error-msg" id="locationInput-error"></div>

<!-- Hire volume field error (line 5275) -->
<div class="field-error-msg" id="hireVolume-error"></div>

<!-- Competitor field error (line 5387) -->
<div class="field-error-msg" id="competitorInput-error"></div>
```

---

### 3. Backend Validation (app.py)

**Location: `/app.py` lines 9680-9734**

#### Budget Validation
```python
# ── CRITICAL: Validate budget is explicitly set (Ashlie Issue #1) ──
_budget_input = str(
    data.get("budget") or "" or data.get("budget_range") or "" or ""
).strip()
if not _budget_input or _budget_input == "":
    self._send_error(
        "Budget must be specified. Please select a budget range or enter an exact amount.",
        "VALIDATION_ERROR",
        400,
    )
    return
```

#### Location Validation
```python
# ── CRITICAL: Validate locations are valid (Ashlie Issue #2) ──
_invalid_locations = ["hell", "test", "xxx", "fake", "placeholder", "n/a"]
if isinstance(_locs_input, list):
    for loc in _locs_input:
        loc_lower = str(loc or "").lower().strip()
        if loc_lower in _invalid_locations:
            self._send_error(
                f'Location "{loc}" is not valid. Please enter a real city, state, or country.',
                "VALIDATION_ERROR",
                400,
            )
            return
        # Reject locations that are too short or have no letters
        if len(loc_lower) < 2 or not any(c.isalpha() for c in loc_lower):
            self._send_error(
                f'Location "{loc}" is invalid. Please use a real city, state, or country name.',
                "VALIDATION_ERROR",
                400,
            )
            return
```

#### Hire Volume Validation
```python
# ── CRITICAL: Validate hire_volume is reasonable (Ashlie Issue #3) ──
_hire_vol = data.get("hire_volume") or ""
if _hire_vol:
    # Extract number from strings like "100 hires" or "100-500"
    import re as _re_module
    _hire_match = _re_module.search(r"(\d+)", str(_hire_vol))
    if _hire_match:
        _hire_num = int(_hire_match.group(1))
        if _hire_num <= 0:
            self._send_error(
                "Hire volume must be greater than 0.",
                "VALIDATION_ERROR",
                400,
            )
            return
        if _hire_num > 100000:
            self._send_error(
                "Hire volume exceeds maximum (100,000).",
                "VALIDATION_ERROR",
                400,
            )
            return
```

---

## CSS Styling Reference

These classes are already defined in the stylesheet and control error display:

```css
/* Error state styling */
.form-group.field-error input,
.form-group.field-error select,
.form-group.field-error textarea {
  border-color: var(--error) !important;
}

/* Error message display */
.field-error-msg {
  color: var(--error);
  font-size: 12px;
  margin-top: 4px;
  display: none;
}

.form-group.field-error .field-error-msg {
  display: block;
}

/* Shake animation */
@keyframes shake {
  0%, 100% { transform: translateX(0); }
  25% { transform: translateX(-5px); }
  75% { transform: translateX(5px); }
}

.form-group.field-error .tags-container {
  border-color: var(--error) !important;
  animation: shake 0.4s ease;
}
```

---

## Integration Points

1. **User fills form** → `validateBudgetInput()`, `validateLocations()`, etc. run
2. **User clicks "Generate Media Plan"** → All validations run in `generatePlan()`
3. **Validation fails** → `showFormError()` displays error message + shake
4. **Validation passes** → Form submits to `/api/generate`
5. **Backend validation** → Secondary check in app.py, returns 400 if invalid

---

## Error Handling Flow

```
Frontend Validation Fails
    ↓
showFormError(fieldId, message) called
    ↓
- Add "field-error" class to form-group
- Display error message div
- Trigger shake animation (0.4s)
- Show toast notification
    ↓
generatePlan() returns early (does not submit)
    ↓
User sees red border + error message + animation
    ↓
User corrects input
    ↓
User clicks "Generate Media Plan" again
```

---

## Testing the Validation

### Test Budget Validation
```javascript
// Open browser console and run:
validateBudgetInput("50000")  // Should return { valid: true, error: "" }
validateBudgetInput("50K")    // Should return { valid: true, error: "" }
validateBudgetInput("0")      // Should return { valid: false, error: "Budget must be greater than 0" }
```

### Test Location Validation
```javascript
// Open browser console and run:
validateLocations(["San Francisco CA"])  // Should return { valid: true, errors: [] }
validateLocations(["hell"])              // Should return { valid: false, errors: [...] }
validateLocations([])                    // Should return { valid: false, errors: [...] }
```

### Test Backend Validation
```bash
# Test with invalid budget (curl)
curl -X POST https://media-plan-generator.onrender.com/api/generate \
  -H "Content-Type: application/json" \
  -d '{"budget":"","client_name":"Test","requester_name":"Test","requester_email":"test@example.com","target_roles":["Engineer"],"locations":["SF"]}'

# Should return 400: "Budget must be specified..."
```

---

## Deployment Steps

1. Deploy `/templates/index.html` changes (frontend validation)
2. Deploy `/app.py` changes (backend validation)
3. Test with invalid inputs to ensure errors are caught
4. Monitor logs for validation errors (should be user-friendly, not 500 errors)
5. Verify no regression in valid form submissions
