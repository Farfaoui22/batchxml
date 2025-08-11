package fr.dsirc.gce.batch.car.persistence.dao;

import java.util.List;

import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import fr.dsirc.fmk.commons.dao.CustomJpaRepository;
import fr.dsirc.gce.cpt.core.persistence.model.Creance;

public interface CreancesCandidatesRevisionDAO extends CustomJpaRepository<Creance, Long> {

	/**
	 * @param idRcUniqueEtab
	 * @param itsMailleEchange
	 * @param exerciceAfferance
	 * @return
	 */
	@Query("SELECT DISTINCT cc FROM Creance cc " //
			+ "LEFT JOIN FETCH cc.mouvements mvts " //
			+ "LEFT JOIN FETCH mvts.detailsMouvement dm " //
			+ "WHERE cc.exerciceAfference=:exerciceAfferance  "
			+ "AND  cc.codeTypeConstatSolde = 'COT' " //
			+ "AND cc.itMailleEchange IN (:itsMailleEchange) " //
			+ "AND cc.mtSolde >= 0 " //
			+ "AND (cc.codeNatureConstatSolde IN('DEC', 'DTP', 'ETP', 'EST','EEC','DER') ) ")
	List<Creance> rechercheCreancesCandidate( @Param("itsMailleEchange") Long[] itsMailleEchange,
			@Param("exerciceAfferance") final int exerciceAfferance);

}
