# Contributing to the Commission Preview ETL

## Code Principles

This ETL project follows clean code principles with an emphasis on:

1. **Simplicity over complexity** - The simplest solution that works is preferred
2. **Single responsibility** - Each component should do one thing well
3. **Current requirements first** - Focus on immediate needs, not future possibilities
4. **Test-driven development** - Where possible, write tests before implementation

## Git Workflow

1. **Branch naming**:
   - Feature branches: `feature/short-description`
   - Bug fixes: `fix/short-description`

2. **Commit messages**:
   - Start with a verb (Add, Fix, Update, Refactor)
   - Be specific but concise
   - Example: "Add Salesforce account extraction query"

3. **Pull requests**:
   - Provide clear description of changes
   - Reference any related issues
   - Ensure all tests pass

## Development Environment

1. Always use a virtual environment
2. Install dependencies from requirements.txt
3. Copy `.env.example` to `.env` and fill in your credentials
4. Never commit credentials or the `.env` file

## Testing Changes

1. Use the `--manual` flag for testing without connecting to real data sources:
   ```
   python run_etl.py --manual
   ```

2. Check logs for errors and inconsistencies

## Important Security Notes

- Never store credentials in code
- Always use the `.env` file for sensitive information
- The `.gitignore` file is configured to prevent accidental commits of sensitive files
