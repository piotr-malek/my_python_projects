from rank.prefilter import evaluate_title, prefilter_title

INCLUDE = [
    "artificial intelligence engineer",
    "analytics engineer",
    "analytics engineering",
    "ai engineer",
    "data engineer",
    "data engineering",
    "data integration",
    "data integrations",
    "data platform engineer",
    "etl engineer",
    "etl",
    "pipeline",
    "platform engineer",
]
EXCLUDE = [
    "intern",
    "internship",
    "software engineer",
    "software developer",
    "frontend engineer",
    "backend engineer",
    "full stack",
    "fullstack",
    "devops",
    "sre",
    "qa engineer",
    "quality engineer",
    "security engineer",
    "sales engineer",
    "product engineer",
    "recruiter",
    "marketing manager",
    "machine learning",
    "ml engineer",
    "mlops",
]
SENIORITY = [
    "senior",
    "staff",
    "principal",
    "lead",
    "head",
    "director",
    "vp",
    "chief",
    "manager",
    "graduate",
    "working student",
    "trainee",
]


def _passes(title: str, seniority: list[str] | None = SENIORITY) -> bool:
    return prefilter_title(
        title,
        include_keywords=INCLUDE,
        exclude_keywords=EXCLUDE,
        seniority_exclude_keywords=seniority or (),
    )


def test_accepts_data_engineer():
    assert _passes("Data Engineer")


def test_accepts_analytics_engineer():
    assert _passes("Analytics Engineer")


def test_accepts_data_integration():
    assert _passes("Data Integrations Specialist")


def test_word_boundary_excludes_do_not_hit_international():
    assert _passes("International Data Engineer")
    assert _passes("Data Engineer, International Programs")


def test_discipline_exclude_spares_qualified_titles():
    assert _passes("Data Software Engineer")
    assert _passes("AI Software Engineer")
    assert _passes("Data Product Engineer")
    assert _passes("Full Stack Analytics Engineer")


def test_discipline_exclude_drops_unqualified_titles():
    assert not _passes("Software Engineer")
    assert not _passes("Backend Engineer")
    assert not _passes("DevOps Engineer")


def test_hard_excludes_always_drop():
    assert not _passes("Data Engineering Recruiter")
    assert not _passes("Sales Engineer, Data Products")
    assert not _passes("Data Engineer Intern")
    assert not _passes("Summer intern, data team")


def test_ml_titles_are_not_targeted():
    assert not _passes("Machine Learning Engineer")
    assert not _passes("ML Engineer")
    assert not _passes("MLOps Engineer")
    # ML exclusion wins even when an AI/data qualifier appears elsewhere in the title
    assert not _passes("Machine Learning Engineer, Secure AI Lab")
    assert not _passes("Data Engineer, Machine Learning Platform")
    # ...but "html" must not trip the "ml engineer" exclude
    assert not _passes("HTML Engineer")  # fails on include, not on a bogus ML match


def test_seniority_gate_drops_senior_titles():
    assert not _passes("Senior Data Engineer")
    assert not _passes("Staff Analytics Engineer")
    assert not _passes("Principal AI Engineer")
    assert not _passes("Lead, Data Integrations")
    assert not _passes("Head of Data Engineering")
    assert not _passes("Data Engineering Manager")


def test_seniority_gate_optional():
    assert _passes("Senior Data Engineer", seniority=None)


def test_seniority_words_need_boundaries():
    # "leadership"/"management" inside longer words must not trigger the gate
    assert _passes("Data Engineer (thought leadership content)")


def test_qualifier_plus_engineer_matches_without_phrase():
    # No include phrase matches, but data + engineer qualifies
    assert _passes("Engineer, Data Infrastructure")


def test_rejects_generic_engineer():
    assert not _passes("Engineer")
    assert not _passes("Civil Works Engineer")


def test_requires_keyword():
    assert not _passes("Office Coordinator")


def test_reasons_reported():
    kw = dict(include_keywords=INCLUDE, exclude_keywords=EXCLUDE, seniority_exclude_keywords=SENIORITY)
    assert evaluate_title("Senior Data Engineer", **kw).reason == "seniority: senior"
    assert evaluate_title("Software Engineer", **kw).reason == "excluded keyword: software engineer"
    assert evaluate_title("Office Coordinator", **kw).reason == "no target role keyword"
    assert evaluate_title("Data Engineer", **kw).reason is None
