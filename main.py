import tkinter as tk
from tkinter import ttk, messagebox, filedialog
import threading
import time
from NSECSVDownloader import NSECSVDownloader  # Ensure your class is saved in a separate file


class IndustryStrengthApp:
    def __init__(self, root):
        self.root = root
        self.root.title("Industry Strength Analysis")
        self.root.geometry("400x300")

        self.downloader = NSECSVDownloader()

        # File Selection
        ttk.Label(root, text="Select File:").grid(row=0, column=0, padx=10, pady=10, sticky="W")
        self.file_options = ["nifty50.csv", "nifty100.csv", "niftymidcap50.csv", "niftymidcap100.csv",
                             "niftytotalmarket.csv"]
        self.file_selection = tk.StringVar(value=self.file_options[0])
        ttk.Combobox(root, textvariable=self.file_selection, values=self.file_options, state="readonly").grid(row=0,
                                                                                                              column=1,
                                                                                                              padx=10,
                                                                                                              pady=10)

        # Period Input
        ttk.Label(root, text="Enter Period:").grid(row=1, column=0, padx=10, pady=10, sticky="W")
        self.period_input = ttk.Entry(root)
        self.period_input.grid(row=1, column=1, padx=10, pady=10)

        # Run Button
        self.run_button = ttk.Button(root, text="Run Analysis", command=self.run_analysis)
        self.run_button.grid(row=2, column=0, columnspan=2, pady=20)

        # Output Label
        self.output_label = ttk.Label(root, text="")
        self.output_label.grid(row=3, column=0, columnspan=2, padx=10, pady=10)

        # Progress Bar (Initially hidden)
        self.progress = ttk.Progressbar(root, orient="horizontal", length=200, mode="indeterminate")
        self.progress.grid(row=4, column=0, columnspan=2, padx=10, pady=10)
        self.progress.grid_remove()  # Use grid_remove() instead of grid_forget()

        # Folder to save the file (Initially None)
        self.save_folder = None

    def run_analysis(self):
        # Ask the user for a folder location to save the output file
        self.save_folder = filedialog.askdirectory(title="Select Folder to Save Output")

        # If the user cancels the folder selection, exit the function
        if not self.save_folder:
            return

        # Define the default file name
        file_name = "RelativeStrength"
        file_path = f"{self.save_folder}/{file_name}"

        # Show the progress bar and start the animation
        self.progress.grid()
        self.progress.start()

        # Disable the Run Button to prevent multiple clicks
        self.run_button.config(state="disabled")

        # Get user inputs
        selected_file = self.file_selection.get()
        period = self.period_input.get()

        # Start the analysis in a separate thread
        threading.Thread(target=self.process_analysis, args=(selected_file, period, file_path), daemon=True).start()

    def process_analysis(self, selected_file, period, file_path):
        try:
            # Validate period input
            period = int(period)
            if period <= 0:
                raise ValueError("Period must be a positive integer.")

            # Run the analysis (this will be a long-running task)
            self.downloader.create_industry_strength_file(selected_file, file_path, period )

            # After the analysis completes, update the UI in the main thread
            self.root.after(0, self.on_analysis_complete, file_path)

        except ValueError as ve:
            self.root.after(0, self.show_error, str(ve))
        except Exception as e:
            self.root.after(0, self.show_error, str(e))

    def on_analysis_complete(self, file_path):
        # Stop and remove the progress bar
        self.progress.stop()
        self.progress.grid_remove()  # Removes the progress bar from the layout

        # Update the output label
        self.output_label.config(text=f"Analysis completed. Output saved to {file_path}.")
        messagebox.showinfo("Success",
                            f"Analysis completed.\nOutput saved to {file_path}.")

        # Re-enable the Run Button
        self.run_button.config(state="normal")

    def show_error(self, error_message):
        # Stop and remove the progress bar
        self.progress.stop()
        self.progress.grid_remove()

        # Show error message
        messagebox.showerror("Error", f"An error occurred: {error_message}")

        # Re-enable the Run Button
        self.run_button.config(state="normal")


# Run the application
if __name__ == "__main__":
    root = tk.Tk()
    app = IndustryStrengthApp(root)
    root.mainloop()
