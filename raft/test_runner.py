
import pprint
import subprocess as subp
import argparse

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('-n', dest='N',default=50, type=int, action="store", required=False)
    args = parser.parse_args()
    
    passed_tests = args.N
    for i in range(args.N):
        print(f"Running test {i} out of {args.N}...", end="")

        log = subp.run(['go','test','-run','2A'], stdout=subp.PIPE)
        log_lines = str(log.stdout).split("\\n")

        if log_lines[-3] == "PASS":
            print("passed.")
        elif  log_lines[-4] == "FAIL":
            print("failed.")   
            passed_tests -= 1
        else:
            print("unknown.")
            passed_tests -= 1
        
        with open(f"./logs/log.{i}", "w") as logfile:
            pprint.pprint(log_lines, logfile)

    print(f"\n -- Passed {passed_tests}/{args.N}\n")



