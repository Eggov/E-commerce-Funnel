# E-Commerce Conversion Funnel & Behavioral Traffic Analytics
*Advanced Looker Studio & Google BigQuery Case Study*

[![Looker Studio](https://img.shields.io/badge/BI_Tool-Looker_Studio-4285F4?style=flat&logo=google)](https://lookerstudio.google.com/)
[![Google BigQuery](https://img.shields.io/badge/Data_Warehouse-BigQuery-669DF6?style=flat&logo=googlecloud)](https://cloud.google.com/bigquery)
[![SQL](https://img.shields.io/badge/SQL-Advanced_Data_Prep-CC292B?style=flat&logo=SQL)](./src)

---

## 📌 Business Overview & Challenge

A digital commerce retailer faced conversion rate stagnation despite maintaining steady acquisition traffic. Management lacked granular visibility into checkout drop-offs and behavioral frictions across the multi-step purchasing journey.

**The Objective:** Build an end-to-end cloud analytical pipeline using **Google BigQuery SQL** and design a high-density, interactive executive dashboard in **Looker Studio**. The solution maps out a **7-stage e-commerce conversion funnel**, isolates friction points from initial session to purchase, and provides cross-filterable segmentation across devices, landing pages, and traffic campaigns.

🔗 **[Live Interactive Looker Studio Dashboard Link](https://datastudio.google.com/reporting/a28fef3e-5508-4435-a566-0fd0f668eab8)**

---

## 🛠️ Tech Stack & Analytical Competencies

* **Cloud Data Warehousing:** Google BigQuery (processing high-volume, event-level clickstream data).
* **Data Engineering & Preprocessing:** Cloud SQL (Window functions, CTEs, stage routing, string parsing, and dataset aggregation).
* **Business Intelligence (BI):** Looker Studio (custom funnel visual architecture, interactive cross-filtering, UI/UX optimization).
* **CRO & Product Analytics:** Drop-off rate diagnosis, checkout leakage tracking, multi-channel acquisition profiling.

---

## 📊 Executive KPI Snapshot & Funnel Architecture

![Dashboard Overview](./images/dashboard_overview.png)

### 📈 Core Top-Line KPIs
* **Total Sessions (Visits):** `354,857`
* **Qualified Orders:** `38,033`
* **Completed Purchases:** `4,745`
* **Macro Conversion Rate (CR to Purchase):** `1.34%`

---

## 📉 7-Stage Granular Conversion Funnel Analysis

Unlike high-level 3-step models, this architecture tracks customer movement through 7 micro-conversion milestones to isolate precise operational bottlenecks:

![7-Stage Conversion Funnel](./images/detailed_funnel.png)

1. **01 Visit:** `354,857` (100.00%) — Baseline acquisition traffic.
2. **02 View Item:** `75,273` (**21.21%**) — *Primary Discovery Drop-off*: **78.79%** of visitors leave without engaging with a product page.
3. **03 Add to Cart:** `14,904` (**4.20%**) — High-intent buyers adding products to basket.
4. **04 Begin Checkout:** `10,859` (**3.06%**) — Users initiating payment setup.
5. **05 Add Shipping:** `10,859` (**3.06%**) — Zero drop-off between checkout start and shipping setup.
6. **06 Add Payment:** `6,671` (**1.88%**) — *Friction Barrier*: **38.5%** cart leakage observed during shipping-to-payment selection.
7. **07 Purchase:** `4,745` (**1.34%**) — Final transaction completion.

---

## 🔍 Strategic Insights & CRO Recommendations

### 1. The Discovery Friction (Visit ➡️ View Item)
* **Insight:** Only **21.21%** of site sessions reach a product view page. 
* **Action:** Overhaul home page recommendations, search indexing, and category navigation to drive immediate product engagement.

### 2. Payment Gateway Friction (Add Shipping ➡️ Add Payment)
* **Insight:** Significant drop-off from 3.06% to 1.88% occurs at the shipping-to-payment stage.
* **Action:** Audit payment gateway loading speeds, introduce express payment options (Apple Pay / Google Pay / PayPal), and check for hidden shipping cost surprises.

### 3. Device & Tech Infrastructure Leakage
* **Insight:** Desktop commands **58.3%** of sessions, Web/iOS represents **11.5%**, with mobile web accounting for the remaining volume. Mobile conversion velocity trails desktop significantly.
* **Action:** Optimize mobile checkout responsiveness and reduce input fields on smaller viewports.

---

## 🚦 Traffic Acquisition & Landing Page Performance

To isolate marketing efficiency from website UI friction, the dashboard tracks performance down to specific URL structures and campaign sources:

![Traffic & Landing Page Analytics](./images/traffic_and_landing_pages.png)

* **Organic Supremacy:** Organic search and direct traffic drive the highest Add-to-Cart velocity, whereas specific promotional sub-pages (`basket.html`, `store.html`) account for over 80% of total generated revenue (`$198,207`).
* **Temporal Fluctuations:** Time-series tracking reveals distinct revenue spikes during specific engagement windows, allowing marketing teams to optimize ad spend timing.

---

## 🎛️ Interactive BI Architecture & Cross-Filtering UX

A key architectural feature of this Looker Studio report is the elimination of cluttered, traditional dropdown menus in favor of an interactive **Visual Control Panel**:

![Interactive Cross-Filtering Controls](./images/cross_filtering_controls.png)

* **Dynamic Cross-Filtering:** Instead of static filters, micro-visualizations (Donut charts for Devices, Horizontal bars for OS, and Treemaps for Languages) serve a dual purpose: they display categorical breakdowns while acting as instant slice-and-dice filters.
* **Zero-Latency Exploration:** Decision-makers can click directly on an operating system (e.g., `iOS` or `Windows`) or device category (`Mobile`) to dynamically recalculate the entire 7-stage conversion funnel in real time without page reloads.

---

## 📐 Data Processing & SQL Framework (`src/`)

To prevent performance degradation in Looker Studio and eliminate real-time calculation latency, raw event tables were pre-processed and aggregated in **Google BigQuery** using standardized SQL scripts (`src/funnel_transformation.sql`).

* Unified clickstream timestamps into standardized session windows.
* Aggregated conversion events (`view_item`, `add_to_cart`, `begin_checkout`, `purchase`) into structured metrics.
* Cleaned UTM dimensions (`Source`, `Medium`, `Campaign`, `Landing page location`).

---

## 📂 Repository Architecture

```text
E-commerce-Funnel/
├── src/                  # Production SQL queries (BigQuery transformations)
│   └── funnel_transformation.sql
├── dashboard/            # Exported PDF version of the Looker Studio report
│   └── E-commerce_Fun
