from pandas import read_csv, DataFrame, to_numeric
from os import path


def extractTransformLoad(current_path: str) -> None:
  path_current_dir = path.dirname(current_path)
  path_ai_adoption_src = path.join(
    path_current_dir, "data", "raw", "ai_adoption_2024.csv"
  )
  df_ai_adoption = extractAiAdoption(path_ai_adoption_src)
  cleanAiAdoption(df_ai_adoption)
  loadAiAdoption(df_ai_adoption, path_current_dir)


def extractAiAdoption(path_ai_adoption: str) -> DataFrame:
  #! column name starting with an underscore will be remap in cleaning process
  column_names = [
    "timestamp",
    "Gender",
    "Age",
    "_Sector",
    "_Department",
    "Familiarity",
    "Resistance",
    "Openness",
    "Usage",
    "Tools",
    "Motivations",
    "Issues",
    "IssuesDetails",
    "Trust",
    "Concerns",
    "LossOfAutonomy",
    "ReducedThinking",
    "FearOfJobLoss",
    "FearOfJobLossDetails",
    "OtherConcerns",
    "OtherConcernsDetails",
    "Involved",
    "WantToBeInvolved",
    "ImpactOfBeingInvolved",
    "WillingnessForTraining",
    "Suggestions",
  ]
  return read_csv(
    path_ai_adoption,
    delimiter=",",
    header=0,
    index_col=False,
    names=column_names,
  )


def cleanAiAdoption(df_survey: DataFrame) -> DataFrame:
  correctAiAdoption(df_survey)
  remapAiAdoption(df_survey)
  categorizeMultiChoiceQuestions(df_survey)
  addNumericalLikertScale(df_survey)


def correctAiAdoption(df_survey: DataFrame) -> DataFrame:
  # * Remove leading and trailing spaces
  for col in df_survey.select_dtypes(include=["object"]).columns:
    df_survey[col] = df_survey[col].str.strip()
  # * Correct input errors
  df_survey["LossOfAutonomy"] = df_survey["LossOfAutonomy"].replace(
    "Moyenement", "Moyennement"
  )
  df_survey["LossOfAutonomy"] = df_survey["LossOfAutonomy"].replace(
    "Partiellement", "Moyennement"
  )
  df_survey["Tools"] = df_survey["Tools"].replace(
    ["non", "NON", "rien"], "Aucun"
  )
  df_survey["Motivations"] = df_survey["Motivations"].replace(
    ["aucune", "Aucune", "aucun", "Aucun", " ", ""], "Aucune motivation"
  )
  df_survey["Motivations"] = df_survey["Motivations"].replace(
    [
      "Nous n'utilisons passe l'IA",
      "Pas d'IA au travail",
      "je nen ai pas",
      "N'utilise pas l'IA",
    ],
    "Pas d'IA",
  )
  df_survey["Motivations"] = df_survey["Motivations"].replace(
    "Focus pour les humains sur des tâches à forte valeur ajoutée",
    "Suppression des  tâches laborieuses et répétitives",
  )


def remapAiAdoption(df_survey: DataFrame) -> DataFrame:
  # * Remap of industry sector and company department to reduce size :
  sector_remapping = {
    "Technologies de l'information et de la communication (TIC)": "TIC",
    "Telecom": "TIC",
    "Télécommunications": "TIC",
    "Immobilier": "Immobilier et Construction",
    "Construction et génie civil": "Immobilier et Construction",
    "Transport et logistique": "Transport et Logistique",
    "Le ramassage scolaire": "Transport et Logistique",
    "ICNA": "Transport et Logistique",
    "Industrie": "Industrie et Technique",
    "Technique automatismes": "Industrie et Technique",
    "Énergie et environnement": "Énergie et Environnement",
    "Santé et services sociaux": "Santé et Services Sociaux",
    "Finance et assurance": "Finance et Assurance",
    "Services professionnels (consulting, juridique, comptabilité, etc.)": "Services Professionnels",
    "Commerce de détail": "Commerce & Marketing",
    "Commerce de gros": "Commerce & Marketing",
    "Recherche": "Recherche",
    "Éducation et formation": "Formation",
    "Médias et divertissement": "Médias et Divertissement",
    "Tourisme et hôtellerie": "Tourisme et Hôtellerie",
    "Marketing": "Commerce & Marketing",
    "Le même que toi": "Unknown",
  }
  df_survey["Sector"] = df_survey["_Sector"].map(sector_remapping)
  print(
    "remap of industry sector:",
    df_survey["_Sector"].unique().size,
    "to",
    df_survey["Sector"].unique().size,
  )

  department_remapping = {
    "R&D": "R&D",
    "developpement": "R&D",
    "Développement": "R&D",
    "Innovation": "R&D",
    "Finance/Comptabilité": "Finance et Comptabilité",
    "Investissement": "Finance et Comptabilité",
    "Achat": "Achats et Supply Chain",
    "Achats": "Achats et Supply Chain",
    "Supply Chain": "Achats et Supply Chain",
    "Gestion de projet": "Gestion de Projet",
    "coordination opérationnelle": "Gestion de Projet",
    "assistant coordinateur opérationnel": "Gestion de Projet",
    "Ressources Humaines": "RH",
    "Vente/Commercialisation": "Ventes et Marketing",
    "Marketing/Communication": "Ventes et Marketing",
    "Production/Fabrication": "Production",
    "Services techniques": "Services Techniques et Exploitation",
    "Technique": "Services Techniques et Exploitation",
    "Exploitation": "Services Techniques et Exploitation",
    "Gestion immobilière": "Immobilier",
    "IT (Technologies de l'Information)": "TIC",
    "Direction de la Securité des Systèmes d'Information": "TIC",
    "Direction": "Direction et Management",
    "Directeur associé - expert data": "Direction et Management",
    "Juridique": "Juridique",
    "Qualité": "Qualité",
    "Social": "Social",
    "Photographe": "Médias et Divertissement",
    "Regie": "Médias et Divertissement",
    "ACCUEIL": "Administration",
    "Opérationnel": "Opérationnel",
    "Controleur aerien": "Aéronautique et Navigation Aérienne",
    "Secrétariat général": "Administration",
  }
  df_survey["Department"] = df_survey["_Department"].map(department_remapping)
  print(
    "remap of company department:",
    df_survey["_Department"].unique().size,
    "to",
    df_survey["Department"].unique().size,
  )

  support_remapping = {
    "R&D": "opé",
    "developpement": "opé",
    "Développement": "opé",
    "Innovation": "opé",
    "Finance/Comptabilité": "sup",
    "Investissement": "opé",
    "Achat": "opé",
    "Achats": "opé",
    "Supply Chain": "opé",
    "Gestion de projet": "opé",
    "coordination opérationnelle": "opé",
    "assistant coordinateur opérationnel": "opé",
    "Ressources Humaines": "sup",
    "Vente/Commercialisation": "opé",
    "Marketing/Communication": "sup",
    "Production/Fabrication": "opé",
    "Services techniques": "sup",
    "Technique": "opé",
    "Exploitation": "opé",
    "Gestion immobilière": "opé",
    "IT (Technologies de l'Information)": "opé",
    "Direction de la Securité des Systèmes d'Information": "opé",
    "Direction": "dir",
    "Directeur associé - expert data": "dir",
    "Juridique": "sup",
    "Qualité": "opé",
    "Social": "sup",
    "Photographe": "opé",
    "Regie": "sup",
    "ACCUEIL": "sup",
    "Opérationnel": "opé",
    "Controleur aerien": "opé",
    "Secrétariat général": "sup",
  }
  df_survey["SupportDepartment"] = df_survey["_Department"].map(
    support_remapping
  )

  df_survey["Trust"] = to_numeric(df_survey["Trust"], errors="coerce")
  # Modifier les valeurs de 'Trust' de 1 à 5 en 0 à 4
  df_survey["Trust"] = df_survey["Trust"].apply(lambda x: x - 1 if x > 0 else x)


def categorizeMultiChoiceQuestions(df_survey: DataFrame) -> DataFrame:
  #! Multiple choice question need to be convert from string to a set of string
  df_survey["Tools"] = df_survey["Tools"].apply(categorizeTools)
  df_survey["Motivations"] = df_survey["Motivations"].apply(
    lambda x: set([s.strip() for s in x.split(",")])
  )


def categorizeTools(tools_str):
  # Définir des catégories d'outils basées sur des phrases clés
  tool_categories = {
    "Assistant vocal": ["Assistant vocal (ex: Siri", "Google Assistant)"],
    "Reconnaissance faciale/biometrique": [
      "Reconnaissance faciale/biométrique (ex: déverrouillage de téléphone",
      "ordinateur",
      "accès zone restreinte…)",
    ],
    "Language Model": [
      "Traitement automatique du langage : chatbots (chat GPT",
      "Copilot...)",
      "traduction automatique",
      "correction automatique...",
    ],
    "Prise de decision": [
      "Traitement de données et aide à la prise de décision (ex: analyse prédictive",
      "big data",
      "Microsoft Fabric)",
    ],
    "Gestion des relations client": [
      "Systèmes de gestion des relations client (ex: analyser les interactions avec les clients",
      "détecter les tendances",
      "personnaliser les communications",
      "HubSpot IA)",
    ],
    "Autres": ["Autres (à spécifier)"],
  }
  categorized_tools = set()
  remaining_tools = set()
  tools_list = tools_str.split(",")
  for tool in tools_list:
    tool = tool.strip()
    found = False
    for category, keywords in tool_categories.items():
      for keyword in keywords:
        if keyword in tool:
          categorized_tools.add(category)
          found = True
          break
      if found:
        break
    if not found:
      remaining_tools.add(tool)
  return categorized_tools.union(remaining_tools)


def addNumericalLikertScale(df_survey: DataFrame) -> DataFrame:
  #! All non-variable lader columns need to be duplicated with the equivalent variable column
  df_survey["*Familiarity"] = df_survey["Familiarity"].map(
    {"Oui, très familier": 2, "Un peu familier": 1, "Pas du tout familier": 0}
  )
  df_survey["*Openness"] = df_survey["Openness"].map(
    {
      "Très ouvert": 3,
      "Plutôt ouvert": 2,
      "Plutôt réticent": 1,
      "Très réticent": 0,
    }
  )
  df_survey["*Usage"] = df_survey["Usage"].map(
    {"Très souvent": 3, "Parfois": 2, "Rarement": 1, "Jamais": 0}
  )
  df_survey["*LossOfAutonomy"] = df_survey["LossOfAutonomy"].map(
    {"Considérablement": 3, "Moyennement": 2, "Un peu": 1, "Pas du tout": 0}
  )
  df_survey["*ReducedThinking"] = df_survey["ReducedThinking"].map(
    {"Considérablement": 3, "Moyennement": 2, "Un peu": 1, "Pas du tout": 0}
  )
  df_survey["*WillingnessForTraining"] = df_survey[
    "WillingnessForTraining"
  ].map({"Oui, certainement": 2, "Peut-être": 1, "Non, probablement pas": 0})


def loadAiAdoption(df_survey: DataFrame, path_current_dir: str) -> None:
  path_ai_adoption = path.join(
    path_current_dir, "data", "cl_ai_adoption_2024.csv"
  )
  df_survey.to_csv(path_ai_adoption, index=False)
  print("Data saved to:", path_ai_adoption)
