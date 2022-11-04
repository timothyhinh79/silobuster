from manglers import *
import random

# 100 names from organization table
test_names = ['Western Washington University', 'Opportunity Council Island County Back Pack Program', 'Westcoast Counseling and Treatment Center', "DSHS Aging and Long-Term Support Administration's Home and Community Services", 'Alternative Humane Society', 'Sea Mar Community Health Center', 'Depressive and Bipolar Support Alliance Whatcom County', 'Lynden School District', 'Blaine School District', 'Whatcom County Dental Resource List', 'Sehome High School', 'Unity Care NW (Bellingham, Cornwall Ave)', 'River of Life Community Church', 'Blaine Food Bank', 'United Way of Island County', 'Lydia Place', 'Second WakeUp House', 'Hope House', 'Food Lifeline Mobile Food Program in Maple Falls', 'PFLAG Whatcom', 'Little Cheerful Cafe Soup Kitchen ', 'Nooksack Tribe (Genesis II)', 'Lummi Community Services ', 'Friends of Friends Medical Support Fund  ', 'Harmony Elementary School', 'Down syndrome Outreach of Whatcom County', 'Opportunity Council (Whatcom & San Juan)', 'Mental Health Central Access Line ', 'Ferndale School District', 'Whatcom Council on Aging', 'Pioneer Human Services', 'Southside Food Bank', 'NAMI Whatcom', 'Good Cheer Food Bank /Distribution Center ', 'Sean Humphrey House', 'St. Joseph’s Catholic Church Outreach', 'Bridges Treatment and Recovery (Bellingham)', 'Sunrise Services, Inc.', 'Good Cheer Thrift Store Langley ', 'Northwest Regional Council', 'Journey Housing and Payee Services', 'Brigid Collins Family Support Center', 'Nooksack Tribal Education Department ', 'Whatcom County Health Department', 'Lummi Nation', 'Social Security Administration', 'Bloodworks Northwest', 'Answers Counseling', 'Whatcom Kid Insider', 'Washington State Domestic Violence Hotline', 'Blaine Food Bank', 'Island County Health Dept.', 'North Sound Cooling Centers', 'CADA', 'Blaine School District - Play & Learn Group', 'SHIBA Helpline (Statewide Health Insurance Benefits Advisors) - Medicare', 'Toddler Learning Center', 'Bellingham Food Bank', 'Alcoholics Anonymous', 'Bellingham Public Library', 'Compass Health ', 'Veterans Crisis Line', "Saint Paul's Episcopal Church", 'Juvenile Community Justice Center ', 'Whatcom Hospice', 
'Victim Support Services', 'RE Store', 'Lynden Senior Community Center ', 'Oxford House Journey', 'Al-Anon/Alateen\n', 'First Baptist Church - Bellingham', 'Acme Elementary School', 'Northwest Youth Services ', 'Habitat For Humanity and HomeStore - Whatcom County', 'Solid Ground Landlord/Tenant Information & Hotline', 'Habitat for Humanity in Whatcom County', 'Lutheran Counseling Network', 'Meridian School District', 'Project Hope', 'Whatcom Center for Early Learning (Ferndale)', 'CLEAR (Coordinated Legal Education, Advice and Referral) ', 'Satellite Food Bank at Christ the King Community Church', 'Law Advocates  ', 'Office of Mobile and Manufactured Housing', 'Sea Mar Oak Harbor Dental Clinic', 'Sean Humphrey House', 'Maplewood House ', 'Compass Health (Bellingham - McLeod Rd)', 'Whatcom Human Rights Task Force', 'Work Opportunities, Inc.', 'Catholic Community Services (Whatcom County)', 'Point Roberts Food Bank', 'Bellingham Public Library', 'Head Start and Early Head Start, Oak Harbor', 'Volunteer Center of Whatcom County', 'DSHS Division of Child Support', 'Lummi Housing ', 'Whatcom Transportation Authority (WTA)', 'Whatcom Homeschool Association ', 'Arthritis Foundation']

def test_random_remove():
    random.seed(1)
    mangle_prob = 0.02

    mangled_names = [manglers.mangle_org_name.random_remove(name, mangle_prob) for name in test_names]
    mangled_names_changed = [mangled_name for mangled_name, orig_name in zip(mangled_names, test_names) if mangled_name != orig_name]
    mangled_proportion = len(mangled_names_changed) / len(test_names)
    assert mangled_proportion > 0.3 and mangled_proportion < 0.6

def test_random_replace():
    random.seed(1)
    mangle_prob = 0.02

    mangled_names = [manglers.mangle_org_name.random_replace(name, mangle_prob) for name in test_names]
    mangled_names_changed = [mangled_name for mangled_name, orig_name in zip(mangled_names, test_names) if mangled_name != orig_name]
    mangled_proportion = len(mangled_names_changed) / len(test_names)
    assert mangled_proportion > 0.3 and mangled_proportion < 0.6

def test_random_replace():
    random.seed(1)
    mangle_prob = 0.30

    mangled_names = [manglers.mangle_org_name.random_null(name, mangle_prob) for name in test_names]
    mangled_names_changed = [mangled_name for mangled_name, orig_name in zip(mangled_names, test_names) if mangled_name != orig_name]
    mangled_proportion = len(mangled_names_changed) / len(test_names)
    assert mangled_proportion > 0.2 and mangled_proportion < 0.4

def test_mangle_org_name():
    random.seed(1)
    remove_prob = 0.02
    replace_prob = 0.02
    null_prob = 0.02

    # from simulations, roughly 40% were mangled by random_remove and random_replace, so (1-0.02) * (1- 0.4)^2 = 35.28% chance of not being mangled 
    mangled_names = [manglers.mangle_org_name.mangle_org_name(name, remove_prob, replace_prob, null_prob) for name in test_names]
    mangled_names_changed = [mangled_name for mangled_name, orig_name in zip(mangled_names, test_names) if mangled_name != orig_name]
    mangled_proportion = len(mangled_names_changed) / len(test_names)
    assert mangled_proportion > 0.55 and mangled_proportion < 0.75