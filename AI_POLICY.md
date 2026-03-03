# Delta Kernel (Rust) AI Policy

### Overview

We recognize that AI coding assistants are part of many developers' workflows. Thoughtful use of these tools can improve productivity and help contributors explore unfamiliar parts of the codebase. However, delta-kernel-rs implements the Delta Lake protocol with strict correctness requirements, and contributions must reflect genuine understanding of the changes being made.

### Guidelines for Contributors

**Be respectful of reviewers and other contributors**. Reviewing takes time and effort, and changes that are needlessly complex, poorly structured, or bloated make that work harder. Plus, future contributors will have to work with (or around) whatever code you merge. If you're unsure whether your contribution is well-structured or appropriately scoped, seek guidance before investing significant effort. You can open a GitHub issue to discuss your approach, use a draft PR to get early feedback on direction, or ask in the Delta-Users Slack.

**Understand and own your changes.** Every change you push and every review you leave reflects on your professional character and reputation – regardless of whether you used tools like AI. If you use AI tools to assist with code generation, you must fully understand every line of the resulting contribution. You should be able to explain the design, justify implementation choices, and debug issues during review. If you cannot, the contribution is not ready to submit. 

Additionally, please **write your own PR description** and ensure it is crisp, complete, and correct.

**Call out unknowns.** If there are any parts of the change you are less confident about – AI generated or otherwise – leave comments on your own PR explaining the concern and what steps you took to verify correctness. Reviewers can then focus their attention where it matters most.

**Match project conventions.** AI tools often generate code that is stylistically inconsistent with a project. Ensure your contributions follow the conventions used in the rest of the codebase, including PR titles (conventional commit format), doc comments, data model, use of helper/utility functions, and error handling patterns.

**Watch for common AI pitfalls:**

* Protocol-incompatible behavior that looks plausible but violates the Delta spec  
* Incorrect or superficial fixes that mask the real problem  
* Changing correct kernel code to match incorrect test expectations (or vice-versa)  
* Bloated and/or duplicative code (AI agents often struggle with encapsulation and abstraction)  
* Overly verbose, duplicated and/or unnecessary documentation  
* Doc and code comments that are stale or refer to the development process (such as dead-end prototyping attempts or initial implementation bugs that were already fixed) rather than describing the current state of the code and the design behind it. These are often called "temporal references."  
* Unnecessary test cases or test scaffolding, or bloated/duplicated test structure (use helpers\!)

**Disclose copyrighted materials**. Contributors are responsible for ensuring that any copyrighted third-party material appearing in AI-generated output has appropriate attribution and licensing. See the [Linux Foundation's Generative AI Policy](https://www.linuxfoundation.org/legal/generative-ai) for further information on licensing considerations.

### What We Will Not Accept

**Unreviewed AI PRs**. PRs that appear to be raw AI output submitted without meaningful engagement from the author may be closed without review. Maintainers with access to AI tools could generate such code more efficiently themselves, and the contributor gains nothing from the review process.

**Unreviewed AI-assisted comments on issues or PRs.** The same ownership principle applies to review comments as to code: if you use AI tools to help draft a comment or review, you are responsible for its quality, completeness, and accuracy. Review and edit AI-assisted output before posting — do not paste raw AI output as-is, as such comments tend to be formulaic and consume attention without adding value.

Automated bots or agents that post AI-generated content without human review are strictly prohibited, unless explicitly configured by project maintainers (e.g., CI-integrated review tools).

### Why This Matters

**delta-kernel-rs is a protocol implementation where correctness is critical**. A subtle bug can cause data loss or corruption for downstream connectors. Code review is a collaborative process that depends on the author understanding their changes well enough to engage meaningfully with reviewer feedback.

**Our reviewing capacity is limited**. Large PRs that lack the requisite understanding may not get reviewed and may eventually be closed. If you want to contribute but are unfamiliar with the codebase, a high-quality issue with a clear problem statement and reproducible example is often a more valuable starting point than an AI-generated PR out of thin air.

### Disclosures

AI tools were used to refine early drafts of this policy, and the final content was edited, reviewed, and approved by human maintainers.

### Sources

This policy was written with input from:

* [Linux Foundation Generative AI Policy](https://www.linuxfoundation.org/legal/generative-ai)  
* [Matplotlib Contributing Guide — Restrictions on Generative AI Usage](https://matplotlib.org/devdocs/devel/contribute.html?utm_source=chatgpt.com#restrictions-on-generative-ai-usage)  
* [Delta-RS Contributing Guide — AI Generated Code](https://github.com/delta-io/delta-rs/blob/main/CONTRIBUTING.md?utm_source=chatgpt.com#ai-generated-code)  
* [DataFusion Contributor Guide — AI-Assisted Contributions](https://datafusion.apache.org/contributor-guide/index.html?#ai-assisted-contributions)
