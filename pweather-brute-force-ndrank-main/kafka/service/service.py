from typing import Optional
from dtos.consumer_message import CandidatesConsumerMessage, ResultsConsumerMessage
from dtos.producer_message import ProducerMessage


class ServiceBase:
    """Base interface for the service layer, the layer responsible for
    merging the received results
    """    

    def process_results_message(self, message: ResultsConsumerMessage) -> Optional[ProducerMessage]:
        """Main method to process received result messages from the message queue

        Args:
            message (ResultsConsumerMessage): received message

        Returns:
            Optional[ResultsProducerMessage]: message to be submitted or none, if a message should not be sent 
        """
        raise NotImplementedError("Method must be overriden")

    def process_candidates_message(self, message: CandidatesConsumerMessage) -> Optional[ProducerMessage]:
        """Main method to process received candidate messages from the message queue

        Args:
            message (CandidatesConsumerMessage): received message

        Returns:
            Optional[CandidatesConsumerMessage]:  message to be submitted or none, if a message should not be sent 
        """
        raise NotImplementedError("Method must be overriden")