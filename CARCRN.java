package fr.dsirc.gce.batch.car.crn;

import java.util.LinkedHashMap;
import java.util.Map;

import lombok.Getter;
import fr.dsirc.gce.tec.batch.commons.crn.commons.AbstractGceCRN;
import fr.dsirc.gce.tec.batch.commons.crn.commons.GceIntegerCompteur;
import fr.dsirc.gce.utils.dv.CodeDomaineFonctionnel;

/**
 * Classe de gestion des comptes rendus (CRN) du batch CAR
 */
@Getter
public final class CARCRN extends AbstractGceCRN {

	private static final long serialVersionUID = 6606279961522513246L;

	public static final String NB_PAIRES_CREANCE_CANDIDATES_COMPENSATION = "nbPairesCreanceCandidatesCompensation";

	public static final String NB_MOUVEMENTS_REVISION_GENERES = "nbMouvementsRevisionGeneres";

	public static final String NB_MOUVEMENTS_AFFECTATION_REVISION_GENERES = "nbMouvementsAffectationRevisionGeneres";

	/**
	 * Initialisation des compteurs
	 */
	public CARCRN() {
		super();
		addCompteur(new GceIntegerCompteur(NB_PAIRES_CREANCE_CANDIDATES_COMPENSATION, NB_PAIRES_CREANCE_CANDIDATES_COMPENSATION));
		addCompteur(new GceIntegerCompteur(NB_MOUVEMENTS_REVISION_GENERES, NB_MOUVEMENTS_REVISION_GENERES));
		addCompteur(new GceIntegerCompteur(NB_MOUVEMENTS_AFFECTATION_REVISION_GENERES, NB_MOUVEMENTS_AFFECTATION_REVISION_GENERES));
	}

	@Override
	public AbstractGceCRN newInstance() {
		return new CARCRN();
	}

	@Override
	protected Map<String, Boolean> checkCompteurs() {
		Map<String, Boolean> checkCompteurs = new LinkedHashMap<>();

		int nbA = (Integer) get(NB_PAIRES_CREANCE_CANDIDATES_COMPENSATION).getValue();
		int nbB = (Integer) get(NB_MOUVEMENTS_REVISION_GENERES).getValue();
		int nbC = (Integer) get(NB_MOUVEMENTS_AFFECTATION_REVISION_GENERES).getValue();

		boolean aEqualsB = nbA == nbB;
		boolean aEqualsC = nbA == nbC;
		boolean aEqualsBEqualsC = aEqualsB == aEqualsC;
		if (!aEqualsBEqualsC) {
			checkCompteurs.put("GCE-CPT-CAR-NBCRECANCMP (A) <> GCE-CPT-CAR-NBMVTGENREV (B) <> GCE-CPT-CAR-NBMVTGENAFR (C) (" + nbA + " <> " + nbB
					+ " <> " + nbC + ") ? ", aEqualsBEqualsC);
		}
		return checkCompteurs;
	}

	@Override
	public String getCodeFonctionnel() {
		return CodeDomaineFonctionnel.COMPTE.getCode();
	}

}
