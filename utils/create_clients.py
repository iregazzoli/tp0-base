import sys

clients_env_vars = [
    {
        "NOMBRE": "Santiago Lionel",
        "APELLIDO": "Lorca",
        "DOCUMENTO": "30904465",
        "NACIMIENTO": "1999-03-17",
        "NUMERO": "7574"
    },
    {
        "NOMBRE": "Maria Fernanda",
        "APELLIDO": "Gomez",
        "DOCUMENTO": "32985476",
        "NACIMIENTO": "1985-08-12",
        "NUMERO": "1289"
    },
    {
        "NOMBRE": "Carlos Alberto",
        "APELLIDO": "Perez",
        "DOCUMENTO": "34567234",
        "NACIMIENTO": "1977-11-23",
        "NUMERO": "3421"
    },
    {
        "NOMBRE": "Lucia Victoria",
        "APELLIDO": "Martinez",
        "DOCUMENTO": "29547612",
        "NACIMIENTO": "1992-05-04",
        "NUMERO": "8547"
    },
    {
        "NOMBRE": "Federico Andres",
        "APELLIDO": "Ramirez",
        "DOCUMENTO": "31547628",
        "NACIMIENTO": "1989-12-09",
        "NUMERO": "6721"
    }
]

def generate_clients(filename, num_clients):
    with open(filename, 'a') as f:
        for i in range(1, int(num_clients) + 1):
            env_index = (i - 1) % len(clients_env_vars)
            env_data = clients_env_vars[env_index]
            f.write(f"  client{i}:\n")
            f.write(f"    container_name: client{i}\n")
            f.write(f"    image: client:latest\n")
            f.write(f"    entrypoint: /client\n")
            f.write(f"    environment:\n")
            f.write(f"      - CLI_ID={i}\n")
            f.write(f"      - NOMBRE={env_data['NOMBRE']}\n")
            f.write(f"      - APELLIDO={env_data['APELLIDO']}\n")
            f.write(f"      - DOCUMENTO={env_data['DOCUMENTO']}\n")
            f.write(f"      - NACIMIENTO={env_data['NACIMIENTO']}\n")
            f.write(f"      - NUMERO={env_data['NUMERO']}\n")
            f.write(f"    networks:\n")
            f.write(f"      - testing_net\n")
            f.write(f"    depends_on:\n")
            f.write(f"      - server\n")
            f.write(f"    volumes:\n")
            f.write(f"      - ./client/config.yaml:/config.yaml\n")
            f.write(f"      - ./.data/agency-{i}.csv:/data/agency-{i}.csv \n")
            f.write("\n")

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: create_clients.py <output_file> <num_clients>")
        sys.exit(1)
    
    output_file = sys.argv[1]
    num_clients = sys.argv[2]

    if not num_clients.isdigit() or int(num_clients) < 1:
        print("Error: <num_clients> must be a positive integer")
        sys.exit(1)

    generate_clients(output_file, num_clients)
