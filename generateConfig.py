import sys

if __name__ == "__main__":
    NUM_SERVERS = int(sys.argv[1])
    PORT_START = 5000
    with open("config.txt", "w") as f:
        for x in range(PORT_START, PORT_START+NUM_SERVERS):
            f.write(f"127.0.0.1 {x}\n")
