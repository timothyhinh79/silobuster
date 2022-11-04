from manglers import *
import random

# 100 URLs from service table
test_urls = ['https://www.dcyf.wa.gov/services/adoption', 'http://fhdc.org', 'https://www.dshs.wa.gov/esa/community-services-offices/medicare-savings-program', 'https://www.ssa.gov/retire/', 'http://bellinghamfoodbank.org', 'https://www.farmaid.org/blog/farmer-heroes/page/2/', 'http://firstbaptistbellingham.com/connect/tuesday-meal', 'http://workopportunities.org', 'https://www.idealoption.com/clinics/auburn', 'https://www.seamar.org/wic-nutrition.html', 'http://www.bellinghampubliclibrary.org', 'https://www.plannedparenthood.org/get-care/our-services/std-testing-treatment-vaccines', 'http://lni.wa.gov/TradesLicensing/LicensingReq/default.asp', 'http://unitycarenw.org/', 'https://www.dshs.wa.gov/altsa/home-and-community-services/services-help-adult-remain-home', 'https://www.dshs.wa.gov/esa/community-services-offices/apple-health-workers-disabilities-hwd-program', 'https://wadshs.libera.com/Sys7CMSPortal-FCMS-WA/fraud/report.aspx', 'https://www.spokanecd.org', 'https://whatcomfoodnetwork.org', 'https://www.dshs.wa.gov/esa/community-services-offices/medicare-savings-program', 'https://www.rma.usda.gov', 'https://www.dshs.wa.gov/dda/community-residential-services-adults', 'https://www.namiwhatcom.org/basics.html', 'https://www.oppco.org/basic-needs/infants-toddlers/', 'http://www.lni.wa.gov/ClaimsIns/Claims/Benefits/Pension/default.asp', 'https://www.dshs.wa.gov/dda/consumers-and-families/residential-services', 'http://www.brigidcollins.org/foster-vision', 'http://www.lni.wa.gov/claimsinsurance/default.asp', 'https://www.ssa.gov/planners/survivors/ifyou.html', 'http://www.lni.wa.gov/', 'https://www.sno-isle.org/brier', 'https://www.worksourcewa.com', 'https://www.dshs.wa.gov/esa/community-services-offices/basic-food', 'https://www.ssa.gov/retire/', 'https://www.washingtonconnection.org', 'https://www.plannedparenthood.org/learn/abortion', 'https://www.dcyf.wa.gov/services/child-welfare-system/cps', 'https://www.answerscounseling.org/', 'https://www.dcyf.wa.gov/services/at-risk-youth/frs', 'https://evergreengoodwill.org/job-training-and-education/training-center-locations/mount-vernon-job-training-and-education-center', 'http://www.whatcomdrc.org', 'https://www.ssa.gov/planners/survivors/ifyou.html', 'https://www.dshs.wa.gov/node/5765/', 'https://www.dshs.wa.gov/esa/community-services-offices/basic-food', 'https://www.dshs.wa.gov/esa/community-services-offices/basic-food', 'http://www.dvsas.org', 'https://www.dcyf.wa.gov/safety/report-abuse', 'https://lifelineconnections.org/locations/bellingham-office/', 'https://www.dcyf.wa.gov/safety/report-abuse', 'https://www.ssa.gov/retire/', 'https://ccsww.org/get-help/housing/permanent-housing/', 'https://www.ssa.gov/planners/survivors/ifyou.html', 'https://www.dcyf.wa.gov/services/child-welfare-system/cps', 'https://www.dshs.wa.gov/node/5765/', 'http://www.lni.wa.gov/claimsinsurance/default.asp', 'https://www.ssa.gov/benefits/medicare/', 'https://www.washingtonconnection.org', 'https://www.oregonrural.org', 'https://www.btc.edu/CommunityAndBusiness/PublicServices.html#Automotive', '', 'http://arcwhatcom.org/wp/programs/', 'https://www.ssa.gov/work/', 'https://www.washingtonconnection.org/', 'https://www.ssa.gov/disabilityssi/', 'https://www.irs.gov/help/contact-my-local-office-in-washington', 'http://whatcom.edu/academics/learning-options/turning-point', 'https://www.dshs.wa.gov/altsa/home-and-community-services/home-care-adults', 'https://www.plannedparenthood.org/learn/general-health-care', 'https://www.plannedparenthood.org/planned-parenthood-great-northwest-hawaiian-islands/patient/services-transgender-patients', 'http://www.dshs.wa.gov', 'https://www.lummi-nsn.gov/Website.php?PageID=671', 'http://dshs.wa.gov/dvr', 'http://www.workopportunities.org', 'https://www.dshs.wa.gov/node/5765/', 'https://www.dshs.wa.gov/altsa/home-and-community-services/services-help-adult-remain-home', 'https://www.dshs.wa.gov/esa/community-services-offices/medicare-savings-program', 'http://www.seattlegoodwill.org', 'https://www.oppco.org/maplealleyinn', 'https://www.whatcomcounty.us/304/Prosecuting-Attorney', 'https://sunriseservicesinc.com/what-we-do/behavioral-health/chemical-dependency-and-recovery-services', 'http://www.dshs.wa.gov', 'https://www.dshs.wa.gov/altsa/home-and-community-services/report-concerns-involving-vulnerable-adults', 'https://www.dshs.wa.gov/dda', 'http://www.altsa.dshs.wa.gov/APS/', 'https://www.dcyf.wa.gov/services/at-risk-youth/frs', 'http://unitycarenw.org/', 'https://www.dshs.wa.gov/node/5765/', 'https://www.dcyf.wa.gov/services/adoption', 'http://www.lni.wa.gov/ClaimsIns/Claims/Benefits/Pension/default.asp', 'https://www.plannedparenthood.org/health-center/washington/bellingham/98225/bellingham-health-center-2454-91780', 'https://www.plannedparenthood.org/learn/general-health-care', 'http://whatcomhumane.org/adopt', 'https://www.dshs.wa.gov/altsa/home-and-community-services/report-concerns-involving-vulnerable-adults', 'https://www.dcyf.wa.gov/services/child-welfare-system/cps', 'https://www.dcyf.wa.gov/services/child-welfare-system/cps', 'https://www.ssa.gov/retire/', 'https://pioneerhumanservices.org/treatment/centers?tid=17#0', 'https://www.dshs.wa.gov/node/5765/', 'http://lni.wa.gov/', 'https://www.dshs.wa.gov/node/5765/']

def test_remove_www():
    random.seed(1)
    urls_with_www = [url for url in test_urls if 'www.' in url]
    mangle_prob = 0.3

    mangled_urls = [manglers.mangle_url.remove_www(url, mangle_prob) for url in urls_with_www]
    mangled_urls_wo_www = [url for url in mangled_urls if 'www.' not in url]
    mangled_proportion = len(mangled_urls_wo_www) / len(urls_with_www)
    assert mangled_proportion > 0.2 and mangled_proportion < 0.4

def test_remove_scheme():
    random.seed(1)
    urls_with_scheme = [url for url in test_urls if 'http' in url]
    mangle_prob = 0.3

    mangled_urls = [manglers.mangle_url.remove_scheme(url, mangle_prob) for url in urls_with_scheme]
    mangled_urls_wo_scheme = [url for url in mangled_urls if 'http' not in url]
    mangled_proportion = len(mangled_urls_wo_scheme) / len(urls_with_scheme)
    assert mangled_proportion > 0.2 and mangled_proportion < 0.4

def test_remove_s_from_https():
    random.seed(1)
    urls_with_https = [url for url in test_urls if 'https' in url]
    mangle_prob = 0.3

    mangled_urls = [manglers.mangle_url.remove_s_from_https(url, mangle_prob) for url in urls_with_https]
    mangled_urls_wo_https = [url for url in mangled_urls if 'https' not in url]
    mangled_proportion = len(mangled_urls_wo_https) / len(urls_with_https)
    assert mangled_proportion > 0.2 and mangled_proportion < 0.4

def test_append_extra_slash():
    random.seed(1)
    urls_wo_ending_slash = [url for url in test_urls if url and url[-1] != '/']
    mangle_prob = 0.3

    mangled_urls = [manglers.mangle_url.append_extra_slash(url, mangle_prob) for url in urls_wo_ending_slash]
    mangled_urls_w_extra_slash = [url for url in mangled_urls if url[-1] == '/']
    mangled_proportion = len(mangled_urls_w_extra_slash) / len(urls_wo_ending_slash)
    assert mangled_proportion > 0.2 and mangled_proportion < 0.4

def test_mispell_url():
    random.seed(1)
    non_empty_urls = [url for url in test_urls if url]

    # for 2% chance of randomly nullifying entire string and removing/replacing each char, 
    # there is a roughly 80% chance URL would change
    # NOTE: multiple simulations show average proportion to actually be between 50% and 65%...
    mangled_urls = [manglers.mangle_url.mispell_url(url, 0.02, 0.02, 0.02) for url in non_empty_urls]
    mangled_urls_mispelled = [mangled_url for mangled_url, orig_url in zip(mangled_urls, non_empty_urls) if mangled_url != orig_url]
    mangled_proportion = len(mangled_urls_mispelled) / len(non_empty_urls)
    assert mangled_proportion > 0.50

def test_change_domain_extension():
    random.seed(1)
    urls_with_common_tld = []
    for url in test_urls:
        for tld in ['com','org','net','edu','gov','us']:
            if tld in url:
                urls_with_common_tld.append(url)

    mangled_urls = [manglers.mangle_url.change_domain_extension(url, manglers.tld_swap_prob_dict.tld_swap_prob_dict) for url in urls_with_common_tld]
    mangled_urls_w_diff_tld = [mangled_url for mangled_url, orig_url in zip(mangled_urls, urls_with_common_tld) if mangled_url != orig_url]
    mangled_proportion = len(mangled_urls_w_diff_tld) / len(urls_with_common_tld)
    assert mangled_proportion > 0.1 and mangled_proportion < 0.3

def test_mangle_url():
    random.seed(1)
    url_mangling_probs_dict = {
        'www_remove_prob': .22, 
        'scheme_remove_prob': .03, 
        'remove_s_from_https_prob': .68,
        'append_extra_slash_prob': .35,
        # 'change_domain_ext_prob': .05, # arbitrary probability
        'mispell_remove_char_prob': .02, # arbitrary probability
        'mispell_replace_char_prob': .02, # arbitrary probability
        'mispell_null_url_prob': .02 # arbitrary probability
    } 

    # ROUGH ESTIMATE: 78% * 97% * (32% /2) * 65% * 55% * 20% = less than 1% chance of not being mangled 
    mangled_urls = [manglers.mangle_url.mangle_url(url, url_mangling_probs_dict, manglers.tld_swap_prob_dict.tld_swap_prob_dict) for url in test_urls]
    mangled_urls_w_diff = [mangled_url for mangled_url, orig_url in zip(mangled_urls, test_urls) if mangled_url != orig_url]
    mangled_proportion = len(mangled_urls_w_diff) / len(test_urls)
    assert mangled_proportion > 0.80