package fr.dsirc.gce.batch.car.persistence.services;

import java.util.ArrayList;
import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import fr.dsirc.gce.batch.car.persistence.dao.CreancesCandidatesRevisionDAO;
import fr.dsirc.gce.cpt.core.persistence.model.Creance;

@Service
public class CreancesCandidatesRevisionService {

	@Autowired
	private CreancesCandidatesRevisionDAO creancesCandidatesRevisionDAO;

	/**
	 * @param idRcUniqueEtab
	 * @param exerciceAfferance
	 * @param itMailleEchange
	 * @return
	 */
	public List<Creance> rechercheCreancesCandidate( final int exerciceAfferance, final Long... itMailleEchange) {
		List<Creance> listeCreancesCTX = creancesCandidatesRevisionDAO.rechercheCreancesCandidate( itMailleEchange, exerciceAfferance);

		return listeCreancesCTX != null ? listeCreancesCTX : new ArrayList<>();
	}
}
