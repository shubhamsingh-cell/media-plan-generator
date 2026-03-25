# Form Validation Quick Reference

## What Was Fixed

### Issue 1: Budget Validation
**Problem:** Form accepted negative, zero, or missing budget values → backend timeouts

**Solution:**
- Frontend: Client-side budget validation before submission
- Backend: Server-side budget requirement check
- Formats Supported: `$50000`, `50K`, `50M`, `2B`, `$50,000`

**Test Case:**
```javascript
// Frontend validation
validateBudgetInput("50000") // ✓ valid
validateBudgetInput("50K")   // ✓ valid
validateBudgetInput("0")     // ✗ error: "Budget must be greater than 0"
validateBudgetInput("")      // ✗ error: "Budget is required"
```

---

### Issue 2: Location Validation
**Problem:** Form accepted invalid values like "hell" → caused 500 errors

**Solution:**
- Frontend: Validates against invalid patterns and validates format
- Backend: Rejects blacklisted location names
- Supports 50+ countries and US states

**Test Cases:**
```javascript
// Frontend validation
validateLocations(["San Francisco CA"])      // ✓ valid
validateLocations(["hell"])                  // ✗ error: "not valid"
validateLocations(["123"])                   // ✗ error: "no letters"
validateLocations([])                        // ✗ error: "required"
```

---

### Issue 3: Hire Volume Validation
**Problem:** Form accepted invalid hire volumes → triggered timeouts

**Solution:**
- Frontend: Optional field with numeric validation
- Backend: Range validation (0 < x ≤ 100,000)
- Formats Supported: `100`, `100 hires`, `100-500 hires`

**Test Cases:**
```javascript
// Frontend validation
validateHireVolume("100")     // ✓ valid
validateHireVolume("0")       // ✗ error: "must be > 0"
validateHireVolume("")        // ✓ valid (optional)
validateHireVolume("1000000") // ✗ error: "exceeds maximum"
```

---

## Code Location Reference

### Frontend Implementation
**File:** `/templates/index.html`

| Component | Lines | Purpose |
|-----------|-------|---------|
| VALID_US_STATES | 9585-9586 | List of 50 US states + territories |
| VALID_COUNTRIES | 9590-9592 | List of 40+ valid countries |
| validateBudgetInput() | 9594-9620 | Budget validation function |
| validateLocations() | 9623-9670 | Location validation function |
| validateHireVolume() | 9671-9691 | Hire volume validation function |
| validateCompetitors() | 9693-9708 | Competitor validation function |
| showFormError() | 9710-9746 | Error display helper |
| generatePlan() validation | 9747-9811 | Integration into form submission |
| Error message elements | 5147, 5320, 5275, 5387 | HTML error divs |

### Backend Implementation
**File:** `/app.py`

| Component | Lines | Purpose |
|-----------|-------|---------|
| Budget validation | 9680-9690 | Enforces budget is set |
| Location validation | 9692-9711 | Rejects invalid locations |
| Hire volume validation | 9713-9734 | Validates hire count range |

---

## User-Facing Error Messages

### Budget Field
```
"Budget must be greater than 0"
"Budget exceeds maximum allowed value ($1B)"
"Budget must be specified. Please select a budget range or enter an exact amount."
```

### Location Field
```
"At least one location is required"
"Location 'X' is not valid. Please enter a real city, state, or country."
"Location 'X' is invalid. Please use a real city, state, or country name."
```

### Hire Volume Field
```
"Hire volume must be greater than 0"
"Hire volume exceeds maximum (100,000)"
"Hire volume must be a number greater than 0"
```

---

## Validation Flow

```
User fills form
    ↓
User clicks "Generate Media Plan"
    ↓
Frontend validation runs:
    - validateBudgetInput()
    - validateLocations()
    - validateHireVolume()
    - validateCompetitors()
    ↓
Invalid? → Show error message + shake animation → Return early
    ↓
Valid? → POST to /api/generate
    ↓
Backend validation runs:
    - Budget must be set
    - Locations must not be in blacklist
    - Hire volume must be in valid range
    ↓
Invalid? → 400 error response to user
    ↓
Valid? → Generate media plan
```

---

## CSS Classes Used

| Class | Purpose | Behavior |
|-------|---------|----------|
| `field-error` | Applied to invalid field | Red border, shows error message |
| `field-valid` | Applied to valid field | Green checkmark (✓) |
| `field-error-msg` | Error message container | Red text, hidden by default |
| `shake` | Animation for invalid field | 0.4s shake effect |

---

## Testing Checklist

- [ ] Budget field: Empty value rejected
- [ ] Budget field: Zero value rejected
- [ ] Budget field: Valid amounts accepted ($50K, 50000, 50M)
- [ ] Location field: "hell" rejected
- [ ] Location field: Valid cities accepted
- [ ] Location field: Numeric values rejected
- [ ] Location field: Empty locations rejected
- [ ] Hire Volume field: Zero rejected
- [ ] Hire Volume field: Valid numbers accepted
- [ ] Hire Volume field: Optional (empty is OK)
- [ ] Competitors field: Invalid names flagged
- [ ] Form submission prevented on validation error
- [ ] Error messages display with red styling
- [ ] Shake animation plays on error
- [ ] Backend also rejects invalid inputs (400 error)

---

## Deployment Checklist

- [ ] Code syntax verified (Python, JavaScript)
- [ ] No breaking changes to existing functionality
- [ ] Error messages are user-friendly
- [ ] CSS classes already exist (no new styles needed)
- [ ] Backend validation matches frontend
- [ ] Ready for production deployment
