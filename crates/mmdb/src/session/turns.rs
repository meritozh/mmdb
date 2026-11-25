//! Turn and step rules for one session. The log loads the state and persists
//! the admitted change in the same batch as the event.

use crate::{EventKind, TurnEndReason, TurnError, TurnStamp};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(super) struct OpenTurn {
    pub owner: String,
    pub session: String,
    pub number: u64,
    pub run: String,
    pub start_event_id: String,
    pub step: Option<u64>,
}

impl OpenTurn {
    fn stamp(&self) -> TurnStamp {
        TurnStamp {
            number: self.number,
            step: self.step,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub(super) enum Owed {
    Terminal { message_id: String, step: u64 },
    Result { call_id: String, step: Option<u64> },
}

impl Owed {
    pub(super) fn step(&self) -> Option<u64> {
        match self {
            Self::Terminal { step, .. } => Some(*step),
            Self::Result { step, .. } => *step,
        }
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub(super) struct Numbers {
    pub turns: u64,
    pub steps: u64,
}

pub(super) struct TurnState {
    pub open: Option<OpenTurn>,
    pub owed: Vec<Owed>,
    pub numbers: Numbers,
}

pub(super) struct Write<'a> {
    pub kind: &'a EventKind,
    pub run: Option<&'a str>,
    pub message_step: Option<u64>,
}

pub(super) enum Change {
    Nothing,
    Owe(Owed),
    Settle(Owed),
    StartTurn { number: u64, run: String },
    StartStep(u64),
    EndStep,
    EndTurn,
}

pub(super) struct Admitted {
    pub stamp: Option<TurnStamp>,
    pub change: Change,
}

/// Link lookups have already established that a result's call exists and
/// that a linked call's message is available.
pub(super) fn admit(state: &TurnState, write: Write<'_>) -> Result<Admitted, TurnError> {
    let open = state.open.as_ref();
    let writing_turn = || {
        let turn = open.ok_or(TurnError::OutsideTurn)?;
        if write.run != Some(turn.run.as_str()) {
            return Err(TurnError::OtherRun);
        }
        Ok(turn)
    };
    let writing_step = || writing_turn()?.step.ok_or(TurnError::OutsideStep);

    let mut stamp = open.map(OpenTurn::stamp);
    let change = match write.kind {
        EventKind::UserMessage | EventKind::Notice => Change::Nothing,
        EventKind::TurnEnded {
            reason: TurnEndReason::Interrupted | TurnEndReason::Forked,
            ..
        } => return Err(TurnError::ReservedReason),
        EventKind::TurnStarted => {
            let run = write.run.ok_or(TurnError::RunRequired)?;
            if open.is_some() {
                return Err(TurnError::TurnOpen);
            }
            // Neither counter can exceed the containing event's sequence.
            // Sequence overflow rejects the batch before publication.
            let number = state.numbers.turns.saturating_add(1);
            stamp = Some(TurnStamp { number, step: None });
            Change::StartTurn {
                number,
                run: run.to_owned(),
            }
        }
        EventKind::TurnEnded { start_event_id, .. } => {
            let turn = writing_turn()?;
            if start_event_id != &turn.start_event_id {
                return Err(TurnError::OtherTurn);
            }
            if turn.step.is_some() || !state.owed.is_empty() {
                return Err(TurnError::TurnUnfinished);
            }
            Change::EndTurn
        }
        EventKind::StepStarted => {
            let turn = writing_turn()?;
            if turn.step.is_some() {
                return Err(TurnError::StepOpen);
            }
            let step = state.numbers.steps.saturating_add(1);
            stamp = Some(TurnStamp {
                number: turn.number,
                step: Some(step),
            });
            Change::StartStep(step)
        }
        EventKind::StepEnded => {
            let step = writing_step()?;
            if state.owed.iter().any(|owed| owed.step() == Some(step)) {
                return Err(TurnError::StepUnfinished);
            }
            Change::EndStep
        }
        EventKind::AssistantStarted { message_id } => Change::Owe(Owed::Terminal {
            message_id: message_id.clone(),
            step: writing_step()?,
        }),
        EventKind::AssistantTerminal { message_id, .. } => Change::Settle(Owed::Terminal {
            message_id: message_id.clone(),
            step: writing_step()?,
        }),
        EventKind::AssistantMessage | EventKind::AssistantFragment { .. } => {
            writing_step()?;
            Change::Nothing
        }
        EventKind::ToolCall {
            call_id,
            message_id,
            ..
        } => {
            let step = writing_turn()?.step;
            if message_id.is_some() && (step.is_none() || step != write.message_step) {
                return Err(TurnError::OutsideStep);
            }
            Change::Owe(Owed::Result {
                call_id: call_id.clone(),
                step,
            })
        }
        EventKind::ToolResult { call_id, .. } => {
            let owed = state
                .owed
                .iter()
                .find(|item| matches!(item, Owed::Result { call_id: id, .. } if id == call_id))
                .ok_or(TurnError::Answered)?;
            writing_turn()?;
            Change::Settle(owed.clone())
        }
    };
    Ok(Admitted { stamp, change })
}
