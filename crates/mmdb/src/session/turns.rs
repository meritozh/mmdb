//! Turn and step rules for one session. The log loads the state and persists
//! the admitted change in the same batch as the event.

use crate::{
    ContextError, ContextResult, EventKind, MessageStatus, ToolOutcome, TurnEndReason, TurnError,
    TurnStamp,
};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(super) struct OpenTurn {
    pub start: u64,
    pub owner: String,
    pub session: String,
    pub number: u64,
    pub run: String,
    pub start_event_id: String,
    pub owes: u64,
    pub step: Option<OpenStep>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(super) struct OpenStep {
    pub number: u64,
    pub start_event_id: String,
    pub owes: u64,
}

impl OpenTurn {
    fn owing_step(&mut self, item: &Owed) -> Option<&mut OpenStep> {
        self.step
            .as_mut()
            .filter(|step| Some(step.number) == item.step())
    }

    pub(super) fn owe(&mut self, item: &Owed) {
        // Each owed row comes from a distinct event. Sequence overflow rejects the batch.
        self.owes = self.owes.saturating_add(1);
        if let Some(step) = self.owing_step(item) {
            step.owes = step.owes.saturating_add(1);
        }
    }

    pub(super) fn settle(&mut self, item: &Owed) -> bool {
        if self.owes == 0 {
            return false;
        }
        if let Some(step) = self.owing_step(item) {
            if step.owes == 0 {
                return false;
            }
            step.owes -= 1;
        }
        self.owes -= 1;
        true
    }

    fn stamp(&self) -> TurnStamp {
        TurnStamp {
            number: self.number,
            step: self.step.as_ref().map(|step| step.number),
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
    pub numbers: Numbers,
}

pub(super) struct Write<'a> {
    pub kind: &'a EventKind,
    pub run: Option<&'a str>,
    pub message_step: Option<u64>,
    pub owed: Option<Owed>,
}

pub(super) enum Change {
    Nothing,
    Owe(Owed),
    Settle(Owed),
    StartTurn { number: u64, run: String },
    StartStep(u64),
    EndStep,
    EndTurn(TurnEndReason),
}

pub(super) struct Admitted {
    pub stamp: Option<TurnStamp>,
    pub change: Change,
}

pub(super) struct Closing {
    pub event_id: String,
    pub kind: EventKind,
    pub stamp: TurnStamp,
}

/// Owed items arrive in identity order, terminals first and then results.
pub(super) fn close(
    turn: &OpenTurn,
    owed: &[Owed],
    reason: TurnEndReason,
) -> ContextResult<Vec<Closing>> {
    if turn.owes != owed.len() as u64 {
        return Err(ContextError::Corrupt(
            "turn owed count disagrees with rows".into(),
        ));
    }
    if let Some(step) = &turn.step {
        let count = owed
            .iter()
            .filter(|item| item.step() == Some(step.number))
            .count();
        if step.owes != count as u64 {
            return Err(ContextError::Corrupt(
                "step owed count disagrees with rows".into(),
            ));
        }
    }
    if reason == TurnEndReason::Completed && (turn.step.is_some() || !owed.is_empty()) {
        return Err(ContextError::Turn(TurnError::TurnUnfinished));
    }
    let prefix = format!("mmdb:turn-{}", turn.number);
    let stamp = turn.stamp();
    let mut closing = Vec::new();
    for item in owed {
        let (tag, identity, kind) = match item {
            Owed::Terminal { message_id, .. } => (
                "terminal",
                message_id,
                EventKind::AssistantTerminal {
                    message_id: message_id.clone(),
                    status: MessageStatus::Interrupted,
                },
            ),
            Owed::Result { call_id, .. } => (
                "result",
                call_id,
                EventKind::ToolResult {
                    call_id: call_id.clone(),
                    outcome: ToolOutcome::EffectUnknown,
                },
            ),
        };
        let hash = blake3::hash(identity.as_bytes());
        closing.push(Closing {
            event_id: format!("{prefix}:{tag}-{hash}"),
            kind,
            stamp,
        });
    }
    if let Some(step) = &turn.step {
        closing.push(Closing {
            event_id: format!("{prefix}:step-end"),
            kind: EventKind::StepEnded {
                start_event_id: step.start_event_id.clone(),
            },
            stamp,
        });
    }
    if matches!(reason, TurnEndReason::Interrupted | TurnEndReason::Forked) {
        closing.push(Closing {
            event_id: format!("{prefix}:end"),
            kind: EventKind::TurnEnded {
                reason,
                start_event_id: turn.start_event_id.clone(),
            },
            stamp: TurnStamp {
                step: None,
                ..stamp
            },
        });
    }
    Ok(closing)
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
    let writing_step = || writing_turn()?.step.as_ref().ok_or(TurnError::OutsideStep);

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
        EventKind::TurnEnded {
            start_event_id,
            reason,
        } => {
            let turn = writing_turn()?;
            if start_event_id != &turn.start_event_id {
                return Err(TurnError::OtherTurn);
            }
            stamp = Some(TurnStamp {
                number: turn.number,
                step: None,
            });
            Change::EndTurn(*reason)
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
        EventKind::StepEnded { start_event_id } => {
            let step = writing_step()?;
            if start_event_id != &step.start_event_id {
                return Err(TurnError::OtherStep);
            }
            if step.owes > 0 {
                return Err(TurnError::StepUnfinished);
            }
            Change::EndStep
        }
        EventKind::AssistantStarted { message_id } => Change::Owe(Owed::Terminal {
            message_id: message_id.clone(),
            step: writing_step()?.number,
        }),
        EventKind::AssistantTerminal { .. } => {
            writing_step()?;
            write.owed.map_or(Change::Nothing, Change::Settle)
        }
        EventKind::AssistantMessage | EventKind::AssistantFragment { .. } => {
            writing_step()?;
            Change::Nothing
        }
        EventKind::ToolCall {
            call_id,
            message_id,
            ..
        } => {
            let step = writing_turn()?.step.as_ref().map(|step| step.number);
            if message_id.is_some() && (step.is_none() || step != write.message_step) {
                return Err(TurnError::OutsideStep);
            }
            Change::Owe(Owed::Result {
                call_id: call_id.clone(),
                step,
            })
        }
        EventKind::ToolResult { .. } => {
            let owed = write.owed.ok_or(TurnError::Answered)?;
            writing_turn()?;
            Change::Settle(owed)
        }
    };
    Ok(Admitted { stamp, change })
}
