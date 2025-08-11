/**
 *
 */
package fr.dsirc.gce.batch.car.processor;

import java.util.List;
import java.util.Set;

import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.StepExecutionListener;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.beans.factory.annotation.Autowired;

import fr.dsirc.gce.batch.car.persistence.services.CAROrchestratorService;
import fr.dsirc.gce.cpt.core.persistence.model.Creance;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

/**
 * Processeur batch Compenser Avant Recouvrement<br>
 * Service S033 - Générer les Mouvements de Compensation<br>
 * @author GIRC
 */
@Slf4j
public class CARGenererMvtsCompProcessor implements ItemProcessor<List<Long>, Set<Creance>>, StepExecutionListener {

	@Autowired
	private CAROrchestratorService orchestratorService;

	@Setter
	private StepExecution stepExecution;

	@Override
	public Set<Creance> process(final List<Long> idCreances) {
		log.debug("GCE - CPT - S032 - Identifier les Constat Créance Candidats à Compensation");
		return orchestratorService.lancerTraitement(stepExecution, idCreances);
	}

	@Override
	public ExitStatus afterStep(final StepExecution stepExecution) {
		return stepExecution.getExitStatus();
	}

	@Override
	public void beforeStep(final StepExecution stepExecution) {
		this.stepExecution = stepExecution;

	}

}
