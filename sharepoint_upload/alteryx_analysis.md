🔍 Deep Dive: Alteryx XML-Based SharePoint Upload Workflow (for AI Agent Reference)

This document provides a line-by-line, tool-by-tool breakdown of the XML configuration from your Alteryx Designer Desktop workflow responsible for uploading monthly commission statements to agent-specific SharePoint folders.

Designed for: AI agents, automation builders, and Python engineers who will reimplement or maintain the workflow outside of Alteryx.

🎯 Workflow Goal

To generate raw Excel files from a filtered SQL dataset (based on a Run_YM field), group them by agency, and upload one file per agency into their respective local SharePoint-synced folders, with validation and safety logic enforced.

🧠 Primary Data Inputs

A. SQL Data Pull (via Dynamic Input)

Table Queried: Combined_Items_Payout_ItemVariables

Filter Applied: [Run_YM] = 202406

Source Columns Pulled: ~70+ fields including:

RPM_Agency_ID, Agency_Name, Account_Number, Supplier_Name, Product_Name, Gross_Commission, Bill_Month, etc.

B. Salesforce Data Pull (via Python + SOQL)

Object Queried: Agency__c

SOQL Logic:

SELECT Id, Name, Agency_ID__c, Commissions_Folder_Name__c
FROM Agency__c
WHERE IsDeleted = FALSE AND Agency_ID__c != ''

Purpose: Used to determine which SharePoint subfolder each agency's file should be placed in.

🧩 Tool Functions and Flow (Detailed by Tool ID)

🔹 ToolID 43 — Text Input: SOQL Setup

Injects Object Name and SOQL Query per row.

🔹 ToolID 44 — Python Tool: SOQL Execution

Loops through ToolID 43 inputs.

Uses simple-salesforce to execute the query.

Outputs records per object with ObjectName field attached.

Returns an empty DataFrame if a query fails.

🔹 ToolID 93 — Select Tool: Input Column Pruning

Selects only needed fields from the SQL and SOQL merges.

🔹 ToolID 105 — Container: Path Constructor

Encapsulates all logic for generating file output paths. Includes:

ToolID 104 — Formula for [PathCharacterCount]

ToolID 102 — Formula for [FullPath]

"C:\\Users\\FrankAhearn\\Bridgepointe Technologies, LLC\\The Signal - Commissions\\Agency Folders - Commissions" + "/" + [Agency_Commission_Folder] + "/" + left(ToString([Run YM]),4) + "/" + ToString([Run YM]) + "_" + [RPM_Payout_Agency_ID] + ".xlsx"

🔹 ToolID 70 — Message Tool: Path Length Check

Halts the workflow using ErrorStop type if PathCharacterCount > 255.

🔹 ToolID 75 — Filter + Message: Folder Existence

Checks if Agency_Commission_Folder is null.

If true, shows a message and stops workflow with:

No Agency Commission Folder Found
- Check Salesforce Agency to make sure field is populated correctly
- Check SharePoint to see if folder exists for agent

🔹 ToolID 25 — Output Tool: Excel Writer

Writes to file path defined by [FullPath]

Excel sheet: sheet1

Output mode: Overwrite existing files/sheets

Does NOT preserve Excel formatting or template logic

🔹 ToolID 107 — Python Tool: File Validation Logger

Uses os.path.exists() to verify if written files exist.

Appends to audit log with:

FullPath

Exists (Boolean)

Checked_At (Timestamp)

🔐 Enforced Business Rules and Guardrails

Rule

Enforced In

Enforcement Type

File path must be ≤ 255 characters

ToolID 104 + 70

ErrorStop Message

Folder must be defined in Salesforce

ToolID 75

Filter + Message

Null folders block downstream write

ToolID 75

Stops file generation

File naming convention enforced

ToolID 102

Formula ({RunYM}_{AgencyID}.xlsx)

Output 1 file per RPM_Agency_ID

Summarize Grouping

Group By before Output

One worksheet per file only

ToolID 25

Static config

Logs confirm file write outcome

ToolID 107

Python validation

📦 Output File Behavior (Alteryx)

File Name Template: {RunYM}_{RPM_Payout_Agency_ID}.xlsx

Output Path Format:

C:/Users/FrankAhearn/Bridgepointe Technologies, LLC/The Signal - Commissions/
└── Agency Folders - Commissions/
    └── {Agency_Commission_Folder}/
        └── {Year}/
            └── {RunYM}_{Agency_ID}.xlsx

Sheet Name: sheet1

Overwrite Mode: Yes

Multi-File Output Enabled: Yes (writes based on [FullPath])

🔎 Special Handling & Overrides

If Agency_Group = "Jett Enterprises + Subs", override folder to:
Jett Enterprises, Inc. (ACH)

If Agency_Group contains Complete Communications, folder becomes:
Complete Communications

These overrides ensure correct mapping when Salesforce data uses group labels.

🧾 Suggested Features to Replicate in Python

Feature

Python Approach

SQL + SOQL input merging

pandas.merge() on RPM_Agency_ID

Path length validation

if len(full_path) > 255: log + skip

Folder presence check

if pd.isnull(folder): log + skip

File grouping

df.groupby('RPM_Agency_ID')

Write-to-disk confirmation

os.path.exists(file)

Filename template enforcement

f"{RunYM}_{agency_id}.xlsx"

Sheet name + overwrite behavior

to_excel(..., sheet_name='sheet1')

File log CSV

Write logs/{RunYM}_upload_log.csv

📌 Final Notes for Reimplementation

The Alteryx workflow is modular, sequential, and defensive.

Any rebuild should maintain the following key principles:

🔒 No file writes without validation

✅ One file per agency with clear pathing

📋 Logs before and after all actions

🧯 Guardrails around long paths and missing folders

This doc serves as the blueprint for any Python-based replica and is designed for AI agents and human developers alike.

