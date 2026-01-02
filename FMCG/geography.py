import random

# =====================================================
# PHILIPPINES REGIONAL GEOGRAPHY (OFFICIAL)
# Region → Province → Key Cities / Municipalities
# =====================================================

PH_GEOGRAPHY = {
    "Region I - Ilocos": {
        "Ilocos Norte": ["Laoag"],
        "Ilocos Sur": ["Vigan"],
        "La Union": ["San Fernando"],
        "Pangasinan": ["Dagupan", "San Carlos", "Urdaneta"]
    },

    "Region II - Cagayan Valley": {
        "Batanes": ["Basco"],
        "Cagayan": ["Tuguegarao"],
        "Isabela": ["Ilagan"],
        "Nueva Vizcaya": ["Bayombong"],
        "Quirino": ["Cabarroguis"]
    },

    "Region III - Central Luzon": {
        "Aurora": ["Baler"],
        "Bataan": ["Balanga"],
        "Bulacan": ["Malolos"],
        "Nueva Ecija": ["Palayan", "Cabanatuan"],
        "Pampanga": ["San Fernando"],
        "Tarlac": ["Tarlac City"],
        "Zambales": ["Olongapo"]
    },

    "Region IV-A - CALABARZON": {
        "Cavite": ["Tagaytay", "Dasmariñas"],
        "Laguna": ["Santa Rosa", "Biñan", "San Pedro"],
        "Batangas": ["Batangas City", "Lipa"],
        "Rizal": ["Antipolo"],
        "Quezon": ["Lucena"]
    },

    "Region IV-B - MIMAROPA": {
        "Occidental Mindoro": ["Mamburao"],
        "Oriental Mindoro": ["Calapan"],
        "Marinduque": ["Boac"],
        "Romblon": ["Romblon"],
        "Palawan": ["Puerto Princesa"]
    },

    "Region V - Bicol": {
        "Albay": ["Legazpi"],
        "Camarines Norte": ["Daet"],
        "Camarines Sur": ["Naga"],
        "Catanduanes": ["Virac"],
        "Masbate": ["Masbate City"],
        "Sorsogon": ["Sorsogon City"]
    },

    "Region VI - Western Visayas": {
        "Aklan": ["Kalibo"],
        "Antique": ["San Jose de Buenavista"],
        "Capiz": ["Roxas City"],
        "Iloilo": ["Iloilo City"],
        "Negros Occidental": ["Bacolod"]
    },

    "Region VII - Central Visayas": {
        "Bohol": ["Tagbilaran"],
        "Cebu": ["Cebu City", "Lapu-Lapu", "Mandaue"],
        "Negros Oriental": ["Dumaguete"],
        "Siquijor": ["Siquijor"]
    },

    "Region VIII - Eastern Visayas": {
        "Biliran": ["Naval"],
        "Eastern Samar": ["Borongan"],
        "Leyte": ["Tacloban"],
        "Northern Samar": ["Catarman"],
        "Samar": ["Catbalogan"],
        "Southern Leyte": ["Maasin"]
    },

    "Region IX - Zamboanga Peninsula": {
        "Zamboanga del Norte": ["Dipolog"],
        "Zamboanga del Sur": ["Pagadian"],
        "Zamboanga Sibugay": ["Ipil"]
    },

    "Region X - Northern Mindanao": {
        "Bukidnon": ["Malaybalay"],
        "Camiguin": ["Mambajao"],
        "Lanao del Norte": ["Iligan"],
        "Misamis Occidental": ["Oroquieta"],
        "Misamis Oriental": ["Cagayan de Oro"]
    },

    "Region XI - Davao Region": {
        "Davao de Oro": ["Nabunturan"],
        "Davao del Norte": ["Tagum"],
        "Davao del Sur": ["Digos"],
        "Davao Occidental": ["Malita"],
        "Davao Oriental": ["Mati"]
    },

    "Region XII - SOCCSKSARGEN": {
        "Cotabato": ["Kidapawan"],
        "Sarangani": ["Alabel"],
        "South Cotabato": ["Koronadal", "General Santos"],
        "Sultan Kudarat": ["Isulan"]
    },

    "Region XIII - Caraga": {
        "Agusan del Norte": ["Butuan"],
        "Agusan del Sur": ["Bayugan"],
        "Surigao del Norte": ["Surigao City"],
        "Surigao del Sur": ["Tandag"],
        "Dinagat Islands": ["San Jose"]
    },

    "Region XIV - BARMM": {
        "Basilan": ["Isabela City"],
        "Lanao del Sur": ["Marawi"],
        "Maguindanao": ["Cotabato City"],
        "Sulu": ["Jolo"],
        "Tawi-Tawi": ["Bongao"]
    },

    "Region XV - NCR": {
        "Metro Manila": [
            "Manila", "Quezon City", "Makati", "Pasig",
            "Taguig", "Mandaluyong", "Muntinlupa",
            "Parañaque", "Marikina", "Caloocan",
            "Las Piñas", "Valenzuela", "Malabon",
            "Navotas", "San Juan", "Pasay", "Pateros"
        ]
    },

    "Region XVI - CAR": {
        "Abra": ["Bangued"],
        "Apayao": ["Kabugao"],
        "Benguet": ["Baguio City"],
        "Ifugao": ["Lagawe"],
        "Kalinga": ["Tabuk"],
        "Mountain Province": ["Bontoc"]
    }
}

def pick_ph_location():
    """Pick a random Philippine location (region, province, city)"""
    region = random.choice(list(PH_GEOGRAPHY.keys()))
    province = random.choice(list(PH_GEOGRAPHY[region].keys()))
    city = random.choice(PH_GEOGRAPHY[region][province])
    return region, province, city
