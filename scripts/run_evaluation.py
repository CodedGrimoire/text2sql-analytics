import os
import subprocess
import datetime

OUTPUT = "EVALUATION.md"

def run_tests():
    print("🚀 Running pytest with coverage...")
    subprocess.run(["pytest", "--cov=src", "--cov-report=html"], check=False)

def generate_eval_report():
    now = datetime.datetime.utcnow().isoformat()
    with open(OUTPUT, "w") as f:
        f.write(f"# Evaluation Report\n\nGenerated: {now}\n\n")
        f.write("## Test Results\n\n")
        res = subprocess.run(["pytest", "-q"], capture_output=True, text=True)
        f.write("```\n" + res.stdout + "\n```\n\n")
        f.write("## Coverage Report\n\nOpen `htmlcov/index.html` for details.\n")

if __name__ == "__main__":
    run_tests()
    generate_eval_report()
    print(f"✅ Evaluation report written to {OUTPUT}")
