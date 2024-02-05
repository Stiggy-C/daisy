package io.openenterprise.daisy

import org.springframework.stereotype.Component

@Component("mvelDrivenOperation")
class MvelDrivenOperationImpl: AbstractMvelDrivenOperationImpl<Any>(Any::class.java)