# AI Scout: System Blueprint & Agent Guidance

AI Scout is an **evidence-grade environmental monitoring platform**. It transforms raw satellite and sensor data into auditable, shareable intelligence. 

## 🎯 Core Mission
Every line of code and every piece of data must adhere to the **Evidence Chain**. We do not just "process data"; we build a "provenance-backed audit trail."

---

## 🏗️ System Architecture (The Four Pillars)

### 1. Ingestion Layer (`/ingestion`)
* **Purpose:** Interface with Sentinel Hub API (Sentinel-1 SAR & Sentinel-2 Optical).
* **Standard:** Every download must generate a `provenance.json` containing: `source_url`, `product_id`, `acquisition_time`, and `processing_level`.

### 2. Analysis Engine (`/analytics`)
* **Purpose:** Compute indices (NDVI, NDWI, NBR) and detect changes.
* **Standard:** Use NumPy and Rasterio. Calculations must include a "Confidence Score" based on cloud masking and sensor noise.

### 3. Evidence Management (`/evidence`)
* **Purpose:** Maintain the audit log and verify data integrity.
* **Standard:** Every result must be hashed (SHA-256). Store all metadata in a STAC-compliant format.

### 4. Reporting Module (`/reporting`)
* **Purpose:** Generate professional "Evidence Pack" PDFs.
* **Standard:** Use ReportLab. Reports must include a map preview, a methodology section, and a digital signature footer.

---

## 🤖 Agent Roles & Swarm Protocol
When assigned a role, prioritize your specific pillar while respecting the cross-pillar standards.

* **@Data-Engineer:** Responsible for `/ingestion`. Focus on API efficiency and rate-limiting.
* **@ML-Engineer:** Responsible for `/analytics`. Focus on algorithmic transparency.
* **@Evidence-Engineer:** Responsible for `/evidence` and `/reporting`. Focus on the "Final Output."
* **@QA-Engineer:** Writes unit tests for all pillars. Ensures "Evidence Grade" means 100% reliability.

---

## 🛠️ Operational Rules (CRITICAL)
1.  **Atomic Branches:** Never commit to `main`. Create a branch for every session: `feat/[role]-[feature-name]`.
2.  **No "Ghost" Code:** Every function must have a docstring explaining its source and logic for the audit trail.
3.  **Dependency Control:** Prefer `sentinelhub`, `rasterio`, `numpy`, `geopandas`, and `reportlab`. Do not add new heavy libraries without justification.
4.  **Security:** Never commit API keys. Use `.env` variables as defined in `bridge.py`.

---

