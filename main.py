#!/usr/bin/env python
import sys
import warnings

from datetime import datetime
from main_crew.src.main_crew.crew import MainCrew

warnings.filterwarnings("ignore", category=SyntaxWarning, module="pysbd")

# This main file is intended to be a way for you to run your
# crew locally, so refrain from adding unnecessary logic into this file.
# Replace with inputs you want to test with, it will automatically
# interpolate any tasks and agents information

def run():
    """
    Run the crew.
    """
    inputs = {
       "question": "What is 'مطعم كومار'?"
    }
    
    try:
       result = MainCrew().crew().kickoff(inputs=inputs)
       print(result.json_dict)
    except Exception as e:
        raise Exception(f"An error occurred while running the crew: {e}")

if __name__ == "__main__":
    run()
