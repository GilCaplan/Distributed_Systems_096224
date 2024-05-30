import os
import nbformat
from nbconvert import HTMLExporter

#C:\Users\USER\AppData\Local\Programs\MiKTeX
def tsv_to_html(tsv_path, html_path):
    with open(tsv_path, 'r') as tsv_file:
        lines = tsv_file.readlines()

    html_content = '<html>\n<head>\n<title>TSV to HTML</title>\n</head>\n<body>\n'
    html_content += '<table border="1">\n'
    for line in lines:
        html_content += '<tr>\n'
        columns = line.strip().split('\t')
        for column in columns:
            html_content += f'<td>{column}</td>\n'
        html_content += '</tr>\n'
    html_content += '</table>\n'
    html_content += '</body>\n</html>'

    with open(html_path, 'w') as html_file:
        html_file.write(html_content)

def convert_notebook_to_html(notebook_path, html_path):
    with open(notebook_path, 'r', encoding='utf-8') as f:
        nb = nbformat.read(f, as_version=4)

    # Remove 'execution_count' from code cells to bypass validation error
    for cell in nb.cells:
        if cell.cell_type == 'code':
            cell['execution_count'] = None

    html_exporter = HTMLExporter()

    # Convert the notebook to HTML without executing code cells
    (body, resources) = html_exporter.from_notebook_node(nb, execute=False)

    with open(html_path, 'w', encoding='utf-8') as f:
        f.write(body)

def combine_html_files(tsv_html_path, notebook_html_path, combined_html_path):
    with open(tsv_html_path, 'r') as tsv_html_file:
        tsv_html = tsv_html_file.read()

    with open(notebook_html_path, 'r') as notebook_html_file:
        notebook_html = notebook_html_file.read()

    combined_html = f"{tsv_html}\n{notebook_html}"

    with open(combined_html_path, 'w') as combined_html_file:
        combined_html_file.write(combined_html)


if __name__ == "__main__":
    tsv_path = '/reports/give_in/output_pokemons.tsv'
    notebook_path = '/reports/give_in/DB_Report_01.ipynb'
    tsv_html_path = 'C:\\Users\\USER\\PycharmProjects\\DataBase_Spring\\reports\\output_pokemons.html'
    notebook_html_path = 'C:\\Users\\USER\\PycharmProjects\\DataBase_Spring\\reports\\DB_Report_01.html'
    combined_html_path = '/reports/give_in/combined_output.html'

    if os.path.exists(tsv_path):
        tsv_to_html(tsv_path, tsv_html_path)
        print(f"Converted {tsv_path} to {tsv_html_path}")
    else:
        print(f"The file {tsv_path} does not exist.")

    if os.path.exists(notebook_path):
        convert_notebook_to_html(notebook_path, notebook_html_path)
        print(f"Converted {notebook_path} to {notebook_html_path}")
    else:
        print(f"The file {notebook_path} does not exist.")

    combine_html_files(notebook_html_path, tsv_html_path, combined_html_path)
    print(f"Combined HTML file saved at {combined_html_path}")