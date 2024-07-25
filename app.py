import os
from clickhouse_driver import Client
from faker import Faker
from faker.providers import internet
import json
import random

faker = Faker()
faker.add_provider(internet)

# Configure ClickHouse client
client = Client(host='127.0.0.1', port=9000)

# Device family, categories, OS family, and browser family definitions
deviceFamily = ["Smartphones", "Tablets", "Laptops", "Desktops", "Smartwatches", "Gaming Consoles", "Smart TVs", "Virtual Reality Headsets", "Smart Home Devices", "Wearable Fitness Trackers", "E-Readers", "Digital Cameras", "Printers", "Projectors", "Network Routers", "Streaming Devices", "Portable Media Players", "GPS Navigation Systems", "Drones", "Smart Speakers"]
deviceCategories = ["Mobile Devices", "Computing Devices", "Wearable Devices", "Home Entertainment", "Networking Devices", "Smart Home Devices", "Peripheral Devices", "Gaming Devices", "Imaging Devices", "Audio Devices", "Health and Fitness Devices", "Virtual Reality Devices", "Industrial Devices", "Automotive Devices", "Security Devices"]
osFamilies = ["Windows", "macOS", "Linux", "iOS", "Android", "Chrome OS", "Unix", "FreeBSD", "Solaris", "Windows Server", "watchOS", "tvOS", "Ubuntu", "Debian", "Red Hat Enterprise Linux", "CentOS", "Fedora", "OpenSUSE", "Arch Linux", "Kali Linux"]
browserFamilies = ["Chrome", "Firefox", "Safari", "Edge", "Opera", "Internet Explorer", "Brave", "Vivaldi", "Tor Browser", "UC Browser", "Samsung Internet", "Puffin Browser", "Epic Privacy Browser", "Dolphin Browser", "Yandex Browser", "Maxthon", "Slimjet", "Waterfox", "SeaMonkey", "Lynx"]

# UUID Array
uuidArray = [faker.uuid4() for _ in range(40)]

# Function to convert an object to a string representation suitable for SQL
def objectToSqlString(obj):
    return json.dumps(obj).replace('"', "'")

# Function to get a random object from an array and convert it to SQL-ready string
def randomObjectToSqlString(array):
    randomIndex = random.randint(0, len(array) - 1)
    randomObject = array[randomIndex]
    return objectToSqlString(randomObject)

# Agent version generator
def generateAgentVersion():
    major = random.randint(0, 9)
    minor = random.randint(0, 9)
    patch = random.randint(0, 9)
    return f"{major}.{minor}.{patch}"

# HTTP version generator
def generateHttpVersion():
    major = random.randint(1, 2)
    minor = random.randint(0, 9)
    return f"HTTP/{major}.{minor}"

# Log message generator
def generateLogMessage():
    return {
        "timestamp": faker.iso8601(),
        "level": random.choice(['info', 'error', 'warn']),
        "message": faker.word(),
        "component": random.choice(['frontend', 'backend', 'database'])
    }

# Function to generate fake log data
def generateFakeLogData(batch_size, total_count):
    batch = []
    for i in range(total_count):
        date = faker.date_time_between(start_date="-30d", end_date="now")
        log = {
            "eventDate": date.date().isoformat(),
            "eventDateTime": date.isoformat().split('.')[0],
            "nanosecond": random.randint(0, 999999999),
            "retention": random.randint(1, 365),
            "accountId": random.choice(uuidArray),
            "agentVersion": generateAgentVersion(),
            "hostId": faker.uuid4(),
            "hostname": faker.domain_name(),
            "dockerId": faker.uuid4(),
            "tags": [faker.word(), faker.word()],
            "eventId": faker.uuid4(),
            "source": faker.domain_word(),
            "service": faker.domain_word(),
            "message": json.dumps(generateLogMessage()),
            "normalizedMessage": json.dumps(generateLogMessage()),
            "severity": random.choice(['info', 'warn', 'error', 'debug']),
            "urlHost": faker.domain_name(),
            "urlMethod": faker.http_method(),
            "urlScheme": random.choice(['http', 'https']),
            "urlPath": faker.url(),
            "urlPort": random.randint(80, 8080),
            "statusCode": random.randint(100, 599),
            "httpVersion": generateHttpVersion(),
            "referer": faker.url(),
            "userAgent": faker.user_agent(),
            "deviceFamily": random.choice(deviceFamily),
            "deviceCategory": random.choice(deviceCategories),
            "osFamily": random.choice(osFamilies),
            "osMajor": str(random.randint(1, 15)),
            "osMinor": str(random.randint(0, 9)),
            "osPatch": str(random.randint(0, 9)),
            "browserFamily": random.choice(browserFamilies),
            "browserMajor": str(random.randint(1, 15)),
            "browserMinor": str(random.randint(0, 9)),
            "browserPatch": str(random.randint(0, 9)),
            "browserPatchMinor": str(random.randint(0, 9)),
            "clientIp": faker.ipv4(),
            "duration": random.randint(1, 5000),
            "bytesWritten": random.randint(100, 10000),
            "contexts": [
                {"key1": "value1", "key2": "value2"},
                {"key3": "value3", "key4": "value4"},
                {"key5": "value5", "key6": "value7"}
            ],
            "attributes": [
                {"key1": "value1", "key2": "value2"},
                {"key3": "value3", "key4": "value4"},
                {"key5": "value5", "key6": "value7"}
            ],
            "bytes": random.randint(100, 1000),
            "distTxnId": faker.uuid4(),
            "distTraceId": faker.uuid4(),
            "distSpanId": faker.uuid4()
        }
        batch.append(log)
        if len(batch) == batch_size:
            insert_logs(batch)
            batch = []

    # Insert remaining logs if any
    if batch:
        insert_logs(batch)

def insert_logs(logs):
    values = []
    for log in logs:
        values.append(f"""
            ('{log['eventDate']}', 
            '{log['eventDateTime']}', 
            {log['nanosecond']}, 
            {log['retention']}, 
            '{log['accountId']}', 
            '{log['agentVersion']}', 
            '{log['hostId']}', 
            '{log['hostname']}', 
            '{log['dockerId']}', 
            ['{"', '".join(log['tags'])}'], 
            '{log['eventId']}', 
            '{log['source']}', 
            '{log['service']}', 
            '{log['message']}', 
            '{log['normalizedMessage']}', 
            '{log['severity']}', 
            '{log['urlHost']}', 
            '{log['urlMethod']}', 
            '{log['urlScheme']}', 
            '{log['urlPath']}', 
            {log['urlPort']}, 
            {log['statusCode']}, 
            '{log['httpVersion']}', 
            '{log['referer']}', 
            '{log['userAgent']}', 
            '{log['deviceFamily']}', 
            '{log['deviceCategory']}', 
            '{log['osFamily']}', 
            '{log['osMajor']}', 
            '{log['osMinor']}', 
            '{log['osPatch']}', 
            '{log['browserFamily']}', 
            '{log['browserMajor']}', 
            '{log['browserMinor']}', 
            '{log['browserPatch']}', 
            '{log['browserPatchMinor']}', 
            '{log['clientIp']}', 
            {log['duration']}, 
            {log['bytesWritten']}, 
            {randomObjectToSqlString(log['contexts'])}, 
            {randomObjectToSqlString(log['attributes'])}, 
            '{log['distTraceId']}', 
            '{log['distTxnId']}', 
            '{log['distSpanId']}')
        """)

    query = f"""
        INSERT INTO logs2.events_v3(
            eventDate, eventDateTime, nanosecond, retention, accountId, 
            agentVersion, hostId, hostname, dockerId, tags, eventId, 
            source, service, message, normalizedMessage, severity, 
            urlHost, urlMethod, urlScheme, urlPath, urlPort, 
            statusCode, httpVersion, referer, userAgent, deviceFamily, 
            deviceCategory, osFamily, osMajor, osMinor, 
            osPatch, browserFamily, browserMajor, browserMinor, browserPatch, 
            browserPatchMinor, clientIp, duration, bytesWritten, context, 
            attributes, distTraceId, distTxnId, distSpanId
        ) VALUES {', '.join(values)}
    """
    client.execute(query)


if __name__ == "__main__":
    try:
        generateFakeLogData(batch_size=1000, total_count=1000000)
        print("data inserted...")
    except KeyboardInterrupt:
        print("Interrupted by user")
