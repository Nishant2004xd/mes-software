import os

# --- Configuration ---

# The name of the final output file
OUTPUT_FILENAME = "project_export.txt"

# List of all the files to be included in the export
FILES_TO_EXPORT = [
    'config.py',
    'requirements.txt',
    'app/__init__.py',
    'app/database.py',
    'app/main.py',
    'app/routes.py',
    'templates/layout.html',
    'templates/productionpl.html',
    'templates/wip.html',
    'templates/attendance.html',
    'templates/dashboard.html',
    'templates/items.html',
    'templates/stages.html',
    'templates/login.html',
    'static/css/attendance.css',
    'static/css/dashboard.css',
    'static/css/items.css',
    'static/css/productionpl.css',
    'static/css/stages.css',
    'static/css/wip.css',
    'static/css/style.css',
    'static/css/login.css',
    'static/js/attendance.js',
    'static/js/dashboard.js',
    'static/js/layout.js',
    'static/js/items.js',
    'static/js/productionpl.js',
    'static/js/stages.js',
    'static/js/wip.js',
    'static/js/script.js', 
]


# --- Main Script Logic ---

def export_project_code():
    """
    Reads all specified project files and combines them into a single text file.
    """
    print(f"--- Starting project export to '{OUTPUT_FILENAME}' ---")

    try:
        # Open the output file in write mode with UTF-8 encoding
        with open(OUTPUT_FILENAME, 'w', encoding='utf-8') as outfile:

            # Loop through each file in the list
            for filepath in FILES_TO_EXPORT:
                print(f"Processing: {filepath}")

                # Write a clear header for each file
                outfile.write("=" * 70 + "\n")
                outfile.write(f"== FILE: {filepath.replace('/', os.sep)}\n")
                outfile.write("=" * 70 + "\n\n")

                try:
                    # Open the source file in read mode and write its content
                    with open(filepath, 'r', encoding='utf-8', errors='ignore') as infile:
                        outfile.write(infile.read())

                except FileNotFoundError:
                    print(f"  -> WARNING: File not found at '{filepath}'. Skipping.")
                    outfile.write(f"--- FILE NOT FOUND: {filepath} ---\n")
                except Exception as e:
                    print(f"  -> ERROR: Could not read file '{filepath}'. Reason: {e}")
                    outfile.write(f"--- ERROR READING FILE: {filepath} ---\n")

                # Add spacing before the next file
                outfile.write("\n\n")

        print(f"\n✅ SUCCESS: All project code has been exported to '{OUTPUT_FILENAME}'.")

    except Exception as e:
        print(f"!!! CRITICAL ERROR: Could not write to output file. Reason: {e}")


# Run the export function when the script is executed
if __name__ == '__main__':
    export_project_code()