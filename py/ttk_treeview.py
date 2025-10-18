# ttk_treeview.py
import tkinter as tk
from tkinter import ttk
from threading import Thread

# Sample data to display in the table
data = [
    ("Alice", 25, "New York"),
    ("Bob", 32, "London"),
    ("Charlie", 45, "Paris"),
    ("Diana", 38, "Tokyo"),
    ("Ethan", 29, "Sydney"),
]

def create_table_view():
    """Creates a basic table using ttk.Treeview."""
    root = tk.Tk()
    root.title("Table View with ttk.Treeview")
    root.geometry("400x250")

    frame = ttk.Frame(root)
    frame.pack(fill="both", expand=True, padx=10, pady=10)

    columns = ("name", "age", "city")
    tree = ttk.Treeview(frame, columns=columns, show="headings")

    tree.heading("name", text="Name")
    tree.heading("age", text="Age")
    tree.heading("city", text="City")

    tree.column("name", width=120, anchor=tk.W)
    tree.column("age", width=60, anchor=tk.CENTER)
    tree.column("city", width=120, anchor=tk.W)

    for row in data:
        tree.insert("", tk.END, values=row)

    scrollbar = ttk.Scrollbar(frame, orient=tk.VERTICAL, command=tree.yview)
    tree.configure(yscrollcommand=scrollbar.set)

    tree.pack(side="left", fill="both", expand=True)
    scrollbar.pack(side="right", fill="y")

    root.mainloop()

if __name__ == "__main__":
    create_table_view()
