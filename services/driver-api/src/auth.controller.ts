import { Body, Controller, HttpCode, Inject, Post, Req, UseFilters } from '@nestjs/common';
import { z } from 'zod';
import { AuthService } from './auth.service';
import { parse, ProblemFilter } from './problem';

const normEmail = z.email().max(254).transform((e) => e.trim().toLowerCase());
const registerBody = z.object({ email: normEmail, password: z.string().min(8).max(128) });
const loginBody = z.object({ email: z.string().max(254).transform((e) => e.trim().toLowerCase()), password: z.string().min(1).max(128) });
const refreshBody = z.object({ refreshToken: z.string().min(20).max(200) });

@Controller('v1/auth')
@UseFilters(ProblemFilter)
export class AuthController {
  constructor(@Inject(AuthService) private readonly auth: AuthService) {}

  @Post('register')
  register(@Body() body: unknown, @Req() req: { ip: string }) {
    const { email, password } = parse(registerBody, body);
    return this.auth.register(email, password, req.ip);
  }

  @Post('login')
  @HttpCode(200)
  login(@Body() body: unknown, @Req() req: { ip: string }) {
    const { email, password } = parse(loginBody, body);
    return this.auth.login(email, password, req.ip);
  }

  @Post('refresh')
  @HttpCode(200)
  refresh(@Body() body: unknown) {
    return this.auth.refresh(parse(refreshBody, body).refreshToken);
  }

  @Post('logout')
  @HttpCode(204)
  async logout(@Body() body: unknown): Promise<void> {
    await this.auth.logout(parse(refreshBody, body).refreshToken);
  }
}
