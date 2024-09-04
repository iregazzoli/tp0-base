import sys

# Array de hashes con las variables de entorno para cada cliente
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
            f.write(f"  client{i}:\n")
            f.write(f"    container_name: client{i}\n")
            f.write(f"    image: client:latest\n")
            f.write(f"    entrypoint: /client\n")
            f.write(f"    environment:\n")
            f.write(f"      - CLI_ID={i}\n")
            f.write(f"      - CLI_LOG_LEVEL=DEBUG\n")
            if i <= len(clients_env_vars): #Just in case
                env_vars = clients_env_vars[i - 1]
                for key, value in env_vars.items():
                    f.write(f"      - {key}={value}\n")
            f.write(f"    networks:\n")
            f.write(f"      - testing_net\n")
            f.write(f"    depends_on:\n")
            f.write(f"      - server\n")
            f.write(f"    volumes:\n")
            f.write(f"      - ./client/config.yaml:/config.yaml\n")
            f.write(f"\n")

if __name__ == "__main__":
    output_file = sys.argv[1]
    num_clients = sys.argv[2]
    generate_clients(output_file, num_clients)
