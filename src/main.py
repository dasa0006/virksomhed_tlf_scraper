import csv, requests, time, random

file_path = "data\\input_data\\cvr_data\\chatgpt opgave leads.csv"

session = requests.Session()
session.headers.update({
    "User-Agent": "Mozilla/5.0",
    "Accept": "application/json"
})

base_url = "https://www.proff.dk/_next/data/I4lFA8MMsWG-90DQZkuNe/search.json?q="

# Read all rows
with open(file_path, newline="", encoding="utf-8-sig") as f:
    reader = list(csv.DictReader(f, delimiter=";"))
    fieldnames = reader[0].keys()

total_requests_this_session = 0
sleep_time_rng_values_list = []

session_requst_iter_limit_rng_value = random.randint(15,55)
print("session_requst_iter_limit_rng_value",session_requst_iter_limit_rng_value)

# Process and write back row by row
for index, row in enumerate(reader):
    cvr = row["CVR"]
    phone = row.get("Telefonnumre", "").strip()

    if phone:
        print(f"✅ Skipping CVR {cvr}")
        continue

    url = f"{base_url}{cvr}"
    print(f"🔍 Querying CVR {cvr}")
    
    try:
        response = session.get(url, timeout=10)
        time.sleep(random.uniform(0.043,0.434))

        total_requests_this_session += 1
        if response.status_code == 200:
            data = response.json()
            hits = data['pageProps']['hydrationData']['searchStore']['companies']['companies']
            if hits:
                phone_number = hits[0].get("phone", "Not Found")
                row["Telefonnumre"] = phone_number if phone_number else "No Match"
            else:
                row["Telefonnumre"] = "No Match"
        else:
            row["Telefonnumre"] = f"Error {response.status_code}"
    except Exception as e:
        row["Telefonnumre"] = f"Exception: {str(e)}"

    print(f"CVR {cvr} Phone {phone_number}")
    time.sleep(random.uniform(0.043,0.434))

    # Write updated row back to file
    reader[index] = row
    with open(file_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, delimiter=";", fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(reader)

    print(f"Total request sent, this session {total_requests_this_session}\n")
    
    time_to_sleep_rng_value = random.uniform(0.443, 3.134)
    sleep_time_rng_values_list.append(time_to_sleep_rng_value)
    list_len = sleep_time_rng_values_list.__len__()
    if list_len >= 2:
        previous_sleep_time_rng_value = sleep_time_rng_values_list[0]
        one_before_previous_sleep_time_rng_value = sleep_time_rng_values_list[1]

        total_sleep_time_value_for_last_three = time_to_sleep_rng_value + previous_sleep_time_rng_value + one_before_previous_sleep_time_rng_value

        if total_sleep_time_value_for_last_three < 1.3:
            time.sleep(random.uniform(3.443, 5.134))
            print("Session Request minor cooldown!\n")
        
        sleep_time_rng_values_list = []

    

    time.sleep(time_to_sleep_rng_value)

    if session_requst_iter_limit_rng_value == total_requests_this_session:
        session_requst_iter_limit_rng_value = random.randint(15,55) + total_requests_this_session
        session.close()
        print("Session Request iteration limit reached! Temporarily shutting down http-session, before relaunch!")
        shutdown_sleep_time_value = random.uniform(35.0, 155.0)
        print(f"Shutting down for {shutdown_sleep_time_value} seconds")
        time.sleep(shutdown_sleep_time_value)

        session = requests.Session()
        session.headers.update({
            "User-Agent": "Mozilla/5.0",
            "Accept": "application/json"
        })

        print("http-session has been reopened!\n")
