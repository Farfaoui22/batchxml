package fr.dsirc.gce.batch.car.persistence.services;

import java.math.BigDecimal;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import fr.dsirc.gce.rcpr.dv.generated.NATURE_CONSTAT_SOLDE;
import fr.dsirc.gce.rcpr.dv.generated.TYPE_MOUVEMENT;
import org.apache.commons.collections.CollectionUtils;
import org.springframework.batch.core.StepExecution;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import fr.dsirc.fmk.batch.context.BatchThreadContext;
import fr.dsirc.gce.cpt.core.dto.ContexteMouvementDTO;
import fr.dsirc.gce.cpt.core.persistence.model.Creance;
import fr.dsirc.gce.cpt.core.persistence.model.dv.CodeNatureSolde;
import fr.dsirc.gce.cpt.core.persistence.model.dv.CodeTypeMouvement;
import fr.dsirc.gce.cpt.core.persistence.model.dv.CodeTypeObjetLie;
import fr.dsirc.gce.cpt.core.persistence.model.mouvement.Mouvement;
import fr.dsirc.gce.cpt.core.persistence.model.mouvement.NumeroPiece;
import fr.dsirc.gce.cpt.core.persistence.service.CreanceService;
import fr.dsirc.gce.cpt.core.persistence.service.NumeroPieceService;
import fr.dsirc.gce.cpt.core.service.CompteTreeService;
import fr.dsirc.gce.rcpr.dv.generated.DOMAINEFONCTIONNEL;
import fr.dsirc.gce.rcpr.dv.generated.NATURE_PROCEDURE;
import fr.dsirc.gce.tec.batch.commons.service.ThrowableLogService;
import fr.dsirc.gce.tec.core.commons.dv.CodeTypeObjetGestion;
import fr.dsirc.gce.tec.core.commons.dv.DVAnomalie;
import fr.dsirc.gce.tec.core.commons.dv.DVAnomalies;
import fr.dsirc.gce.tec.core.commons.persistance.model.Anomalie;
import fr.dsirc.gce.tec.core.commons.persistance.model.ObjetGestionRejete;
import fr.dsirc.gce.tvs.dto.TVSResult;
import fr.dsirc.gce.tvs.service.TVSTraiterVariationSoldeService;
import fr.dsirc.gce.utils.dv.CodeAnomalie;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

import javax.validation.constraints.NotNull;

/**
 * Orchestrateur de services du batch CAR
 * @author U05E750
 */
@Slf4j
@Service
public class CAROrchestratorService {

	@Autowired
	private ThrowableLogService throwableLogService;

	@Autowired
	@Setter
	private BatchThreadContext batchThreadContext;

	@Autowired
	private CreanceService creanceService;

	@Autowired
	private CreancesCandidatesRevisionService creancesCandidatesService;

	@Autowired
	private NumeroPieceService numeroPieceService;

	@Autowired
	private CompteTreeService compteTreeService;

	@Autowired
	private TVSTraiterVariationSoldeService traiterVariationSoldeService;
	private static final Map<String,Integer> mapPrioriteCodeNatureConstatSolde = Stream.of(new Object[][] {
			{NATURE_CONSTAT_SOLDE.DER.getCode(), 1 },
			{NATURE_CONSTAT_SOLDE.DEC.getCode(), 2 },
			{NATURE_CONSTAT_SOLDE.DTP.getCode(), 3 },
			{NATURE_CONSTAT_SOLDE.ETP.getCode(), 4 },
			{NATURE_CONSTAT_SOLDE.EST.getCode(), 5 },
	}).collect(Collectors.toMap(data -> (String) data[0], data -> (Integer) data[1]));
	private static final BigDecimal MIN_REVISION = new BigDecimal("0.008");

	/**
	 * @param stepExecution
	 * @param idCreances
	 * @return
	 */
	public Set<Creance> lancerTraitement(final StepExecution stepExecution, final List<Long> idCreances) {

		Set<Creance> creancesCreditrice = creanceService.findCreancesByIdTechniqueIn(idCreances);

		log.debug("GCE - CPT - RG508 - Détermination des Créances candidates à affectation sur révision");

		List<Creance> listeCreancesCandidate = creancesCandidatesService.rechercheCreancesCandidate(
				 creancesCreditrice.iterator().next().getExerciceAfference(),
				creancesCreditrice.iterator().next().getItMailleEchange());

		// S'il existe au moins une Creance avec le Code Nature Procedure égal à PIN-Procédure Individuelle ou PCO-Procédure Collective, alors ne
		// pas faire de Révision ni d'Affectation sur révision.Sinon, poursuivre.

		List<Creance> listeCreancePCOEtPIN = listeCreancesCandidate.stream()
				.filter(c -> NATURE_PROCEDURE.PCO.getCode().equals(c.getCodeTypeProc()) || NATURE_PROCEDURE.PIN.getCode().equals(c.getCodeTypeProc()))
				.collect(Collectors.toList());

		if (CollectionUtils.isNotEmpty(listeCreancePCOEtPIN)) {
			return null;

		}

		// Les créances non soldées c'est-à-dire les créances dont le montant Solde strictement supérieur à 0 de la plus récente à la plus ancienne en
		// considérant les valeurs Echeance déclaratives

		List<Creance> listeCreancesNonSolde = listeCreancesCandidate.stream()
				.filter(c -> c.getMtSolde().compareTo(MIN_REVISION) > 0 && c.getMtSolde().equals(c.getMontantSoldeAffecte()))
				.collect(Collectors.toList());

		/*
		 * • Puis en fonction de leur code code Nature Constat Solde : EEC-Ecart Engagé / Calculé DEC - Déclaré DTP - Déclaré TP ETP - Estimé TP EST -
		 * Estimé Batch
		 */



		// Creance Candidate dont le montant Solde strictement supérieur à 0 par Ordre
		if (CollectionUtils.isNotEmpty(listeCreancesNonSolde)) {
			listeCreancesNonSolde.sort(creationComparatorCreancesNonSoldees(creancesCreditrice.iterator().next().getIdRcUniqueEtab()));
		}


		List<Creance> listeCreancesSolde =  listeCreancesCandidate.stream()
				.filter((Creance c) -> c.getMtCredit().compareTo(MIN_REVISION) < 0
						&& getSommeAffectations(c).compareTo(MIN_REVISION.negate())<0).collect(Collectors.toList());

		// • Les créances soldées c'est-à-dire les créances dont le montant Solde est égal à 0 de la plus récente à la plus ancienne en considérant
		// les valeurs Echeance déclaratives
		if (CollectionUtils.isNotEmpty(listeCreancesSolde)) {
			listeCreancesSolde.sort(creationComparatorLiberationCredit(creancesCreditrice.iterator().next().getIdRcUniqueEtab()));
		}

		creerMouvementRevisionEtAffectationSurRevision(stepExecution, creancesCreditrice, listeCreancesNonSolde, listeCreancesSolde);

		return creancesCreditrice;
	}

	/**
	 * @param listeCreancesCreditrices
	 * @param listeCreancesNonSolde
	 * @param listeCreancesSolde
	 */
	private void creerMouvementRevisionEtAffectationSurRevision(final StepExecution stepExecution, final Set<Creance> listeCreancesCreditrices,
			final List<Creance> listeCreancesNonSolde, final List<Creance> listeCreancesSolde) {

		if (CollectionUtils.isNotEmpty(listeCreancesNonSolde) || CollectionUtils.isNotEmpty(listeCreancesSolde)) {

			// créer numéro de piéce lié à un couple de Constat Créance

			for (Creance creanceCreditrice : listeCreancesCreditrices) {
				NumeroPiece noPieceValue = numeroPieceService.reserverNumeroPiece();

//<GCE-26696>
//Le montant disponible pour la révision correspond à la valeur absolue de la somme montant Debit + montant Credit Allegement Generaux (on prend la valeur absolue pour faciliter les comparaisons dans la suite du traitement)
// (1) Afin de prendre en compte les problèmes d'arrondis liés aux francs Pacifique
//</GCE26696>
				BigDecimal mnttransmis = creanceCreditrice.getMtDebit().abs().add(creanceCreditrice.getMtCreditAG().abs());
				BigDecimal mntRestant = mnttransmis;
				// Traitement des créances non soldés
				Boolean traite = Boolean.FALSE;
				if (CollectionUtils.isNotEmpty(listeCreancesNonSolde)) {
					for (Creance creanceNonSolde : listeCreancesNonSolde) {
						if (creanceNonSolde.getMtSolde().compareTo(BigDecimal.ZERO) > 0) {
							mntRestant = creanceNonSolde.getMtSolde().subtract(mntRestant.abs());

							mnttransmis = mnttransmis.min(creanceNonSolde.getMtSolde().abs());

							creationMouvementAffectationSurRevision(noPieceValue, mnttransmis, creanceNonSolde, creanceCreditrice,creanceNonSolde);
							creationMouvementDeRevision(creanceCreditrice,creanceNonSolde,mnttransmis,noPieceValue);

							mnttransmis = mntRestant.abs();
							traite = Boolean.TRUE;

							if (mntRestant.compareTo(new BigDecimal(0.008)) > 0) {
								break;
							}

						}
					}
				}

				// Traitement des créances soldés
				/**
				 * Le montant potentiel d'affectation sur révision de ces créances est égale à la somme de leur montant Debit + leur montant Credit
				 * Allegement Generaux
				 */
				BigDecimal mntRestantSolde = BigDecimal.ZERO;
				if (CollectionUtils.isNotEmpty(listeCreancesSolde) && mntRestant.compareTo(BigDecimal.ZERO) < 0
						|| CollectionUtils.isEmpty(listeCreancesNonSolde) && mntRestant.compareTo(BigDecimal.ZERO) > 0) {

					BigDecimal mnttransmisSolde = mntRestant.abs();
					mntRestantSolde = mntRestant.abs();

					for (Creance creanceSolde : listeCreancesSolde) {

						mntRestantSolde = creanceSolde.getMtCredit().abs().subtract(mntRestantSolde.abs());

						mnttransmisSolde = mnttransmisSolde.min(creanceSolde.getMtCredit().abs());

						Mouvement<Creance> mouvement = creationMouvementAffectationSurRevision(noPieceValue, mnttransmisSolde, creanceSolde,creanceCreditrice,creanceSolde);
						creationMouvementDeRevision(creanceCreditrice,creanceSolde,mnttransmisSolde,noPieceValue);

						mntRestant = mntRestant.abs().subtract(mnttransmisSolde);
						TVSResult tvsResult = traiterVariationSoldeService.traiterVariationSolde(mouvement);

						mnttransmisSolde = mntRestantSolde.abs();
						traite = Boolean.TRUE;



						if (mntRestantSolde.compareTo(new BigDecimal(0.008)) >= 0) {
							break;
						}


					}
				}

				BigDecimal montantTotalTransmis = BigDecimal.ZERO;
				BigDecimal mntRestantTotal = mntRestantSolde.compareTo(BigDecimal.ZERO) < 0 ? mntRestantSolde : mntRestant;

				if (mntRestantTotal.compareTo(BigDecimal.ZERO) >= 0) {
					montantTotalTransmis = creanceCreditrice.getMontantSoldeAffecte();
				} else {
					montantTotalTransmis = creanceCreditrice.getMontantSoldeAffecte().subtract(mntRestantTotal);
					// Lorsqu'il reste un montant de siponible --> on crée d'une anomalie Fonctionnelle
				}

				if (Boolean.FALSE.equals(traite)) {
					// Créer un rejet
					creerRejet(stepExecution, creanceCreditrice.getIdTechnique(), CodeAnomalie.GCE_CPT_CAR);
				}



				if (mntRestantTotal.compareTo(BigDecimal.ZERO) < 0) {
					// Créer un rejet
					creerRejet(stepExecution, creanceCreditrice.getIdTechnique(), CodeAnomalie.GCE_CPT_CAR);
				}

			}
		}

		// compteur fonctionnel pour le Nombre de Mouvement Révision générés
	}

	private void creationMouvementDeRevision(Creance creanceCreditrice,Creance creanceCandidate, BigDecimal mnttransmis,NumeroPiece noPieceValue) {
		ContexteMouvementDTO contexteRevision = new ContexteMouvementDTO();
		if(creanceCreditrice.getIdRcUniqueEtab().equals(creanceCandidate.getIdRcUniqueEtab()))
			if(creanceCandidate.getIdRcUniqueEtab().equals(creanceCreditrice.getIdRcUniqueEtab())){
				contexteRevision.setCodeTypeMouvement(CodeTypeMouvement.REVISION);

			}else {
				contexteRevision.setCodeTypeMouvement(CodeTypeMouvement.REVISION_CREDIT_INTER_ETABLISSEMENT);

			}
		contexteRevision.setMontantMouvement(mnttransmis);
		contexteRevision.setMontantObjetAffectation(contexteRevision.getMontantMouvement());
		contexteRevision.setPiece(noPieceValue);
		contexteRevision.setIdObjetLie(null);
		contexteRevision
				.setCodeTypeObjetLie(null);

		compteTreeService.createAndAddMouvement(creanceCreditrice, contexteRevision);
	}


	/**
	 * @param noPieceValue
	 * @param montantTransmis
	 * @param creanceSolde
	 */
	private Mouvement<Creance> creationMouvementAffectationSurRevision(final NumeroPiece noPieceValue, final BigDecimal montantTransmis,
			final Creance creanceSolde,final Creance creanceCreditrice,Creance creanceCandidate) {
		ContexteMouvementDTO contexteAffectationRevision = new ContexteMouvementDTO();
		if(creanceCandidate.getIdRcUniqueEtab().equals(creanceCreditrice.getIdRcUniqueEtab())){
			contexteAffectationRevision.setCodeTypeMouvement(CodeTypeMouvement.AFFECTATION_SUR_REVISION);

		}else{
			contexteAffectationRevision.setCodeTypeMouvement(CodeTypeMouvement.AFFECTATION_SUR_REVISION_INTER_ETAB);

		}
		contexteAffectationRevision.setMontantMouvement(montantTransmis.negate());
		contexteAffectationRevision.setMontantObjetAffectation(contexteAffectationRevision.getMontantMouvement());
		contexteAffectationRevision.setPiece(noPieceValue);
		contexteAffectationRevision.setIdObjetLie(null);
		contexteAffectationRevision.setCodeTypeObjetLie(null);

		Mouvement<Creance> mvt = compteTreeService.createAndAddMouvement(creanceSolde, contexteAffectationRevision);
		return mvt;
	}

	/**
	 * @param stepExecution
	 * @param idCreance
	 * @param codeAnomalie
	 * @return
	 */
	private ObjetGestionRejete creerRejet(final StepExecution stepExecution, final Long idCreance, final String codeAnomalie) {

		// Récupérer le message anomalie du DV
		StringBuilder message = new StringBuilder();
		DVAnomalie dvAnomalie = DVAnomalies.getDVAnomalie(codeAnomalie);
		if (dvAnomalie != null) {
			message.append(dvAnomalie.getLibelleLong());
		} else {
			message.append("Code Anomalie : ");
			message.append(codeAnomalie);
		}

		// Sauvegarder le rejet
		ObjetGestionRejete objetGestionRejete = throwableLogService.createRejectOfCurrentBloc(
				Arrays.asList(new Anomalie(codeAnomalie, message.toString(), "CAR")), Arrays.asList(idCreance.toString()), CodeTypeObjetGestion.CAR,
				DOMAINEFONCTIONNEL.CPT.getCode(), stepExecution);

		return objetGestionRejete;
	}
	public Comparator<Creance> creationComparatorCreancesNonSoldees(final Long idRcuEtabRegularisatrice) {
		return (final Creance c1, final Creance c2) -> {

			// 1. Créances de type EEC
			boolean isC1EEC = c1.getCodeNatureConstatSolde().equals(CodeNatureSolde.ECART_ENGAGE_CALCULE.getCode());
			boolean isC2EEC = c2.getCodeNatureConstatSolde().equals(CodeNatureSolde.ECART_ENGAGE_CALCULE.getCode());

			if (isC1EEC && !isC2EEC) return -1;
			if (!isC1EEC && isC2EEC) return 1;

			// Si les deux sont EEC
			if (isC1EEC && isC2EEC) {
				// 1.1.1 Priorité à l’établissement régularisateur
				boolean isC1Intra = c1.getIdRcUniqueEtab().equals(idRcuEtabRegularisatrice);
				boolean isC2Intra = c2.getIdRcUniqueEtab().equals(idRcuEtabRegularisatrice);
				if (isC1Intra != isC2Intra) {
					return Boolean.compare(isC2Intra, isC1Intra); // intra d'abord
				}

				// 1.1.2 Montant solde décroissant
				return c2.getMtSolde().compareTo(c1.getMtSolde());
			}

			// 1.2 Autres créances

			// 1.2.1 Échéance décroissante
			int comparaisonEcheance = c2.getValeurEcheanceRecouvre().compareTo(c1.getValeurEcheanceRecouvre());
			if (comparaisonEcheance != 0) return comparaisonEcheance;

			// 1.2.2 Priorité à l’établissement régularisateur
			boolean isC1Intra = c1.getIdRcUniqueEtab().equals(idRcuEtabRegularisatrice);
			boolean isC2Intra = c2.getIdRcUniqueEtab().equals(idRcuEtabRegularisatrice);
			if (isC1Intra != isC2Intra) {
				return Boolean.compare(isC2Intra, isC1Intra); // intra d'abord
			}

			// 1.2.3 Priorité métier sur le code nature
			Integer prioriteNatureC1 = mapPrioriteCodeNatureConstatSolde.get(c1.getCodeNatureConstatSolde());
			Integer prioriteNatureC2 = mapPrioriteCodeNatureConstatSolde.get(c2.getCodeNatureConstatSolde());
			int comparaisonNature = prioriteNatureC1.compareTo(prioriteNatureC2);
			if (comparaisonNature != 0) return comparaisonNature;

			// 1.2.4 Montant solde décroissant
			return c2.getMtSolde().compareTo(c1.getMtSolde());
		};
	}
	@NotNull
	private static BigDecimal getSommeAffectations(final Creance creance) {
		return  creance.getMouvements().stream()
				.filter(mvt -> TYPE_MOUVEMENT.RPAFF.getCode().equals(mvt.getCodeTypeMouvement())
						|| TYPE_MOUVEMENT.RPDES.getCode().equals(mvt.getCodeTypeMouvement()))
				.map(Mouvement::getMtMouvement)
				.reduce(BigDecimal::add)
				.orElse(BigDecimal.ZERO);
	}
	public Comparator<Creance> creationComparatorLiberationCredit(final Long idRcuEtabRegularisatrice){
		return (final Creance c1, final Creance c2) -> {

			BigDecimal sommeAffectationsC1 = getSommeAffectations(c1);
			BigDecimal sommeAffectationsC2 = getSommeAffectations(c2);

			//2.1	identifiant RC Unique Etablissement :
			//		 	2.1.1	Du même identifiant RC Unique Etablissement que la Creance régularisatrice en premier
			//			2.1.2	Puis les autres identifiant RC Unique Etablissement de la MIRE triés par ordre croissant
			Boolean isC1MemeEtab = c1.getIdRcUniqueEtab().equals(idRcuEtabRegularisatrice);
			Boolean isC2MemeEtab = c2.getIdRcUniqueEtab().equals(idRcuEtabRegularisatrice);
			if (isC1MemeEtab != isC2MemeEtab && (isC1MemeEtab || isC2MemeEtab)) {
				return isC2MemeEtab.compareTo(isC1MemeEtab);
			}

			//		2.2	Somme des Mouvement de type RPAFF-Affectation et RPDES-Désaffectation croissante (2)
			if (!sommeAffectationsC1.equals(sommeAffectationsC2)) {
				return sommeAffectationsC2.abs().compareTo(sommeAffectationsC1.abs());
			}

			//		2.3	code Nature Constat Solde : EEC - Ecart Engagé Calculé
			Boolean isC1EEC = c1.getCodeNatureConstatSolde().compareTo(CodeNatureSolde.ECART_ENGAGE_CALCULE.getCode()) == 0;
			Boolean isC2EEC = c2.getCodeNatureConstatSolde().compareTo(CodeNatureSolde.ECART_ENGAGE_CALCULE.getCode()) == 0;

			if (isC2EEC.compareTo(isC1EEC) != 0) {
				return isC2EEC.compareTo(isC1EEC);
			}

			//  	2.4	valeur Echeance décroissant (du plus récent au plus ancien)
			int comparaisonValeurEcheance = c2.getValeurEcheanceRecouvre().compareTo(c1.getValeurEcheanceRecouvre());
			if (comparaisonValeurEcheance != 0) {
				return comparaisonValeurEcheance;
			}

			// 2.5	code Nature Constat Solde :
			//	1.2.4	code Nature Constat Solde :
			//			2.5.1	DER - Déclaré Régularisé
			//			2.5.2	DEC - Déclaré
			//			2.5.3	DTP - Déclaré TP
			//			2.5.4	ETP - Estimé TP
			//			2.5.5	EST - Estimé Batch
			Integer prioriteNatureC1 = mapPrioriteCodeNatureConstatSolde.get(c1.getCodeNatureConstatSolde());
			Integer prioriteNatureC2 = mapPrioriteCodeNatureConstatSolde.get(c2.getCodeNatureConstatSolde());

			int comparaisonNature = prioriteNatureC1.compareTo(prioriteNatureC2);
			if (comparaisonNature != 0) {
				return comparaisonNature;
			}

			return 0;
		};
	}
}
