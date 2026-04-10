#!/usr/bin/env python3
"""
Region name mappings based on research of actual places, cities, and landmarks.
This provides meaningful display names instead of generic directional names.
"""

# Mapping of region identifiers to meaningful display names
# Format: (parent_region, subregion_num or None): "Display Name"
REGION_NAME_MAPPINGS = {
    # Brazil - Roraima municipalities
    ("Alto_Alegre", 1): "Alto Alegre - Vila do Céu",
    ("Alto_Alegre", 2): "Alto Alegre - Central",
    ("Alto_Alegre", 3): "Alto Alegre - Eastern Border",
    
    ("Amajari", 1): "Amajari - Serra do Tepequém",
    ("Amajari", 2): "Amajari - Central",
    ("Amajari", 3): "Amajari - Eastern Border",
    
    ("Boa_Vista", 1): "Boa Vista",
    
    ("Bonfim", 1): "Bonfim",
    
    ("Canta", 1): "Cantá",
    
    ("Caracarai", 1): "Caracaraí - Southern",
    ("Caracarai", 2): "Caracaraí - Central",
    ("Caracarai", 3): "Caracaraí - Northern",
    ("Caracarai", 4): "Caracaraí - Eastern",
    ("Caracarai", 5): "Caracaraí - Northeastern",
    ("Caracarai", 6): "Caracaraí - Far Eastern",
    
    ("Caroebe", 1): "Caroebe - Southern",
    ("Caroebe", 2): "Caroebe - Northern",
    
    ("Iracema", 1): "Iracema - Western",
    ("Iracema", 2): "Iracema - Eastern",
    
    ("Mucajai", 1): "Mucajaí",
    
    ("Pacaraima", 1): "Pacaraima - Mount Roraima",
    
    ("Rorainopolis", 1): "Rorainópolis - Southern",
    ("Rorainopolis", 2): "Rorainópolis - Central",
    ("Rorainopolis", 3): "Rorainópolis - Northern",
    
    ("Sao_Joao_da_Baliza", 1): "São João da Baliza",
    
    ("Sao_Luiz", 1): "São Luiz",
    
    ("Uiramuta", 1): "Uiramutã",
    
    # USA - California Counties
    ("Amador_County", 1): "Amador County - Jackson Area",
    
    ("Butte_County", 1): "Butte County - Chico Area",
    
    ("Calaveras_County", 1): "Calaveras County - Angels Camp",
    
    ("Colusa_County", 1): "Colusa County",
    
    ("El_Dorado_County", 1): "El Dorado County - Placerville",
    
    ("Fresno_County", 1): "Fresno County - Western",
    ("Fresno_County", 2): "Fresno County - Eastern",
    
    ("Glenn_County", 1): "Glenn County - Willows",
    
    ("Kern_County", 1): "Kern County - Bakersfield Area",
    ("Kern_County", 2): "Kern County - Central Valley",
    ("Kern_County", 3): "Kern County - Mojave Desert",
    
    ("Kings_County", 1): "Kings County - Hanford",
    
    ("Madera_County", 1): "Madera County",
    
    ("Merced_County", 1): "Merced County",
    
    ("Placer_County", 1): "Placer County - Auburn",
    
    ("Sacramento_County", 1): "Sacramento County",
    
    ("San_Joaquin_County", 1): "San Joaquin County - Stockton",
    
    ("Solano_County", 1): "Solano County - Fairfield",
    
    ("Stanislaus_County", 1): "Stanislaus County - Modesto",
    
    ("Sutter_County", 1): "Sutter County - Yuba City",
    
    ("Tulare_County", 1): "Tulare County - Visalia Area",
    ("Tulare_County", 2): "Tulare County - Eastern",
    
    ("Yolo_County", 1): "Yolo County - Davis",
    
    ("Yuba_County", 1): "Yuba County - Marysville",
    
    # Indonesia - West Kalimantan
    ("Bengkayang", 1): "Bengkayang Regency",
    
    ("Kapuas_Hulu", 1): "Kapuas Hulu - Putussibau",
    ("Kapuas_Hulu", 2): "Kapuas Hulu - Central",
    ("Kapuas_Hulu", 3): "Kapuas Hulu - Eastern",
    ("Kapuas_Hulu", 4): "Kapuas Hulu - Northeastern",
    
    ("Melawi", 1): "Melawi Regency - Nanga Pinoh",
    
    ("Pontianak", 1): "Pontianak Regency",
    
    ("Sambas", 1): "Sambas Regency",
    
    ("Sekadau", 1): "Sekadau Regency",
    
    ("Sintang", 1): "Sintang - Southern",
    ("Sintang", 2): "Sintang - Central",
    ("Sintang", 3): "Sintang - Eastern",
    
    # Bhutan - Districts
    ("Bhutan", 1): "Bhutan - Samtse District",
    ("Bhutan", 2): "Bhutan - Chukha District",
    ("Bhutan", 3): "Bhutan - Paro District",
    ("Bhutan", 4): "Bhutan - Thimphu District",
    ("Bhutan", 5): "Bhutan - Punakha District",
    ("Bhutan", 6): "Bhutan - Wangdue Phodrang",
    ("Bhutan", 7): "Bhutan - Trongsa District",
    ("Bhutan", 8): "Bhutan - Bumthang District",
    
    # Nepal - Himalayan Foothills (Provinces/Districts)
    ("Himalayan_Foothills_Nepal_Bhutan", 1): "Nepal - Far Western Province",
    ("Himalayan_Foothills_Nepal_Bhutan", 2): "Nepal - Mid-Western Province",
    ("Himalayan_Foothills_Nepal_Bhutan", 3): "Nepal - Western Province",
    ("Himalayan_Foothills_Nepal_Bhutan", 4): "Nepal - Central Province",
    ("Himalayan_Foothills_Nepal_Bhutan", 5): "Nepal - Eastern Province",
    ("Himalayan_Foothills_Nepal_Bhutan", 6): "Nepal - Far Eastern Province",
    ("Himalayan_Foothills_Nepal_Bhutan", 7): "Nepal - Bagmati Province",
    ("Himalayan_Foothills_Nepal_Bhutan", 8): "Nepal - Gandaki Province",
    ("Himalayan_Foothills_Nepal_Bhutan", 9): "Nepal - Lumbini Province",
    ("Himalayan_Foothills_Nepal_Bhutan", 10): "Nepal - Karnali Province",
    ("Himalayan_Foothills_Nepal_Bhutan", 11): "Nepal - Sudurpashchim Province",
    ("Himalayan_Foothills_Nepal_Bhutan", 12): "Nepal - Koshi Province",
    ("Himalayan_Foothills_Nepal_Bhutan", 13): "Nepal - Madhesh Province",
    ("Himalayan_Foothills_Nepal_Bhutan", 14): "Nepal - Province 1",
    ("Himalayan_Foothills_Nepal_Bhutan", 15): "Nepal - Province 2",
    ("Himalayan_Foothills_Nepal_Bhutan", 16): "Nepal - Province 3",
    ("Himalayan_Foothills_Nepal_Bhutan", 17): "Nepal - Province 4",
    ("Himalayan_Foothills_Nepal_Bhutan", 18): "Nepal - Province 5",
    ("Himalayan_Foothills_Nepal_Bhutan", 19): "Nepal - Province 6",
    ("Himalayan_Foothills_Nepal_Bhutan", 20): "Nepal - Province 7",
    ("Himalayan_Foothills_Nepal_Bhutan", 21): "Nepal - Dang District",
    ("Himalayan_Foothills_Nepal_Bhutan", 22): "Nepal - Banke District",
    ("Himalayan_Foothills_Nepal_Bhutan", 23): "Nepal - Bardiya District",
    ("Himalayan_Foothills_Nepal_Bhutan", 24): "Nepal - Kailali District",
    ("Himalayan_Foothills_Nepal_Bhutan", 25): "Nepal - Kanchanpur District",
    ("Himalayan_Foothills_Nepal_Bhutan", 26): "Nepal - Doti District",
    ("Himalayan_Foothills_Nepal_Bhutan", 27): "Nepal - Achham District",
    ("Himalayan_Foothills_Nepal_Bhutan", 28): "Nepal - Bajura District",
    ("Himalayan_Foothills_Nepal_Bhutan", 29): "Nepal - Bajhang District",
    ("Himalayan_Foothills_Nepal_Bhutan", 30): "Nepal - Darchula District",
    ("Himalayan_Foothills_Nepal_Bhutan", 31): "Nepal - Baitadi District",
    ("Himalayan_Foothills_Nepal_Bhutan", 32): "Nepal - Dadeldhura District",
    
    # Australia - Northern Territory (using major towns and landmarks)
    ("Australian_Outback_Northern_Territory", 1): "Northern Territory - Katherine Region",
    ("Australian_Outback_Northern_Territory", 2): "Northern Territory - Victoria River",
    ("Australian_Outback_Northern_Territory", 3): "Northern Territory - Daly River",
    ("Australian_Outback_Northern_Territory", 4): "Northern Territory - Adelaide River",
    ("Australian_Outback_Northern_Territory", 5): "Northern Territory - Darwin Region",
    ("Australian_Outback_Northern_Territory", 6): "Northern Territory - Kakadu",
    ("Australian_Outback_Northern_Territory", 7): "Northern Territory - Arnhem Land",
    ("Australian_Outback_Northern_Territory", 8): "Northern Territory - East Arnhem",
    ("Australian_Outback_Northern_Territory", 9): "Northern Territory - Roper River",
    ("Australian_Outback_Northern_Territory", 10): "Northern Territory - McArthur River",
    ("Australian_Outback_Northern_Territory", 11): "Northern Territory - Gulf Country",
    ("Australian_Outback_Northern_Territory", 12): "Northern Territory - Barkly Tableland",
    ("Australian_Outback_Northern_Territory", 13): "Northern Territory - Tanami Desert",
    ("Australian_Outback_Northern_Territory", 14): "Northern Territory - Central Desert",
    ("Australian_Outback_Northern_Territory", 15): "Northern Territory - Simpson Desert",
    ("Australian_Outback_Northern_Territory", 16): "Northern Territory - Finke River",
    ("Australian_Outback_Northern_Territory", 17): "Northern Territory - Alice Springs Region",
    ("Australian_Outback_Northern_Territory", 18): "Northern Territory - MacDonnell Ranges",
    ("Australian_Outback_Northern_Territory", 19): "Northern Territory - West MacDonnell",
    ("Australian_Outback_Northern_Territory", 20): "Northern Territory - East MacDonnell",
    ("Australian_Outback_Northern_Territory", 21): "Northern Territory - Davenport Range",
    ("Australian_Outback_Northern_Territory", 22): "Northern Territory - Plenty Highway",
    ("Australian_Outback_Northern_Territory", 23): "Northern Territory - Sandover Highway",
    ("Australian_Outback_Northern_Territory", 24): "Northern Territory - Barkly Highway",
    ("Australian_Outback_Northern_Territory", 25): "Northern Territory - Stuart Highway North",
    ("Australian_Outback_Northern_Territory", 26): "Northern Territory - Stuart Highway Central",
    ("Australian_Outback_Northern_Territory", 27): "Northern Territory - Stuart Highway South",
    ("Australian_Outback_Northern_Territory", 28): "Northern Territory - Lasseter Highway",
    ("Australian_Outback_Northern_Territory", 29): "Northern Territory - Tanami Road",
    ("Australian_Outback_Northern_Territory", 30): "Northern Territory - Victoria Highway",
    ("Australian_Outback_Northern_Territory", 31): "Northern Territory - Kakadu Highway",
    ("Australian_Outback_Northern_Territory", 32): "Northern Territory - Arnhem Highway",
    
    # Mali - Cercles
    ("Bandiagara_Cercle", 1): "Bandiagara Cercle - Dogon Country",
    
    ("Bankass_Cercle", 1): "Bankass Cercle",
    
    ("Djenne_Cercle", 1): "Djenné Cercle",
    
    ("Douentza_Cercle", 1): "Douentza Cercle - Western",
    ("Douentza_Cercle", 2): "Douentza Cercle - Central",
    ("Douentza_Cercle", 3): "Douentza Cercle - Eastern",
    
    ("Koro_Cercle", 1): "Koro Cercle",
    
    ("Mopti_Cercle", 1): "Mopti Cercle",
    
    ("Tenenkou_Cercle", 1): "Ténenkou Cercle - Western",
    ("Tenenkou_Cercle", 2): "Ténenkou Cercle - Eastern",
    
    ("Youwarou_Cercle", 1): "Youwarou Cercle",
    
    # Niger
    ("Tahoua_Department", 1): "Tahoua Department",
}

def get_display_name(parent_region: str, subregion_num: int = None) -> str:
    """Get display name from mapping, or return None if not found."""
    key = (parent_region, subregion_num) if subregion_num else (parent_region, None)
    return REGION_NAME_MAPPINGS.get(key)
